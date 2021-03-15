package nomad

import (
	"fmt"
	"sort"
	"strings"
	"time"

	metrics "github.com/armon/go-metrics"
	log "github.com/hashicorp/go-hclog"
	memdb "github.com/hashicorp/go-memdb"

	"github.com/hashicorp/nomad/acl"
	"github.com/hashicorp/nomad/nomad/state"
	"github.com/hashicorp/nomad/nomad/structs"
)

const (
	// truncateLimit is the maximum number of matches that will be returned for a
	// prefix for a specific context
	truncateLimit = 20
)

var (
	// ossContexts are the oss contexts which are searched to find matches
	// for a given prefix
	ossContexts = []structs.Context{
		structs.Allocs,
		structs.Jobs,
		structs.Nodes,
		structs.Evals,
		structs.Deployments,
		structs.Plugins,
		structs.Volumes,
		structs.ScalingPolicies,
		structs.Namespaces,
	}
)

// Search endpoint is used to look up matches for a given prefix and context
type Search struct {
	srv    *Server
	logger log.Logger
}

// getPrefixMatches extracts matches for an iterator, and returns a list of ids for
// these matches.
func (s *Search) getPrefixMatches(iter memdb.ResultIterator, prefix string) ([]string, bool) {
	var matches []string

	fmt.Println("SH getPrefixMatches, iter:", iter, "prefix:", prefix)

	for i := 0; i < truncateLimit; i++ {
		raw := iter.Next()
		if raw == nil {
			break
		}

		var id string
		switch t := raw.(type) {
		case *structs.Job:
			id = t.ID
		case *structs.Evaluation:
			id = t.ID
		case *structs.Allocation:
			id = t.ID
		case *structs.Node:
			id = t.ID
		case *structs.Deployment:
			id = t.ID
		case *structs.CSIPlugin:
			id = t.ID
		case *structs.CSIVolume:
			id = t.ID
		case *structs.ScalingPolicy:
			id = t.ID
		case *structs.Namespace:
			id = t.Name
		default:
			matchID, ok := getEnterpriseMatch(raw)
			if !ok {
				s.logger.Error("unexpected type for resources context", "type", fmt.Sprintf("%T", t))
				continue
			}

			id = matchID
		}

		if !strings.HasPrefix(id, prefix) {
			continue
		}

		matches = append(matches, id)
	}

	return matches, iter.Next() != nil
}

func (s *Search) getFuzzyMatches(iter memdb.ResultIterator, text string) (map[structs.Context][]structs.FuzzyMatch, map[structs.Context]bool) {
	limitQuery := s.srv.config.SearchConfig.LimitQuery
	limitResults := s.srv.config.SearchConfig.LimitResults

	fmt.Println("SH GFM limitQuery:", limitQuery, "limitResults:", limitResults)

	unsorted := make(map[structs.Context][]fuzzyMatch)
	truncations := make(map[structs.Context]bool)

	accumulateSet := func(set map[structs.Context][]fuzzyMatch) {
		for ctx, matches := range set {
			for _, match := range matches {
				if len(unsorted[ctx]) < limitResults {
					unsorted[ctx] = append(unsorted[ctx], match)
				} else {
					truncations[ctx] = true
					return
				}
			}
		}
	}

	accumulateSingle := func(ctx structs.Context, match *fuzzyMatch) {
		if match != nil {
			if len(unsorted[ctx]) < limitResults {
				unsorted[ctx] = append(unsorted[ctx], *match)
			} else {
				truncations[ctx] = true
				return
			}
		}
	}

	for i := 0; i < limitQuery; i++ {
		raw := iter.Next()
		if raw == nil {
			break
		}

		switch t := raw.(type) {
		case *structs.Job:
			accumulateSet(s.fuzzyMatchesJob(t, text))
		default:
			accumulateSingle(s.fuzzyMatchSingle(raw, text))
		}
	}

	// sort the set of match results
	for ctx := range unsorted {
		sortSet(text, unsorted[ctx])
	}

	// create the result out of exported types
	m := make(map[structs.Context][]structs.FuzzyMatch, len(unsorted))
	for ctx, matches := range unsorted {
		m[ctx] = make([]structs.FuzzyMatch, 0, len(matches))
		for _, match := range matches {
			m[ctx] = append(m[ctx], structs.FuzzyMatch{
				ID:    match.id,
				Scope: match.scope,
			})
		}
	}

	return m, truncations
}

// fuzzySingleMatch determines if the ID of raw is a fuzzy match with text.
// Returns the context and score or nil if there is no match.
func (s *Search) fuzzyMatchSingle(raw interface{}, text string) (structs.Context, *fuzzyMatch) {
	var (
		name string
		ctx  structs.Context
	)

	switch t := raw.(type) {
	case *structs.Node:
		name = t.Name
		ctx = structs.Nodes
	case *structs.Namespace:
		name = t.Name
		ctx = structs.Namespaces
	case *structs.Allocation:
		name = t.Name
		ctx = structs.Allocs
	case *structs.CSIPlugin:
		name = t.ID
		ctx = structs.Plugins
	}

	if idx := strings.Index(name, text); idx >= 0 {
		return ctx, &fuzzyMatch{
			id:    name,
			score: idx,
			scope: nil, // currently no non-job sub-types need scoping
		}
	}

	return "", nil
}

// getFuzzyMatchesJob digs through j and extracts matches against several types
// of matchable Context. Results are categorized by Context and paired with their
// score, but are unsorted.
//
//   job.name
//   job|group.name
//   job|group|service.name
//   job|group|task.name
//   job|group|task|service.name
//   job|group|task|driver.{image,command,class}
func (*Search) fuzzyMatchesJob(j *structs.Job, text string) map[structs.Context][]fuzzyMatch {
	sm := make(map[structs.Context][]fuzzyMatch)
	ns := j.Namespace
	job := j.Name

	// job.name
	if idx := strings.Index(j.Name, text); idx >= 0 {
		sm[structs.Jobs] = append(sm[structs.Jobs], score(job, ns, idx))
	}

	// job|group.name
	for _, group := range j.TaskGroups {
		if idx := strings.Index(group.Name, text); idx >= 0 {
			sm[structs.Groups] = append(sm[structs.Groups], score(group.Name, ns, idx, job))
		}

		// job|group|service.name
		for _, service := range group.Services {
			if idx := strings.Index(service.Name, text); idx >= 0 {
				sm[structs.Services] = append(sm[structs.Services], score(service.Name, ns, idx, job, group.Name))
			}
		}

		// job|group|task.name
		for _, task := range group.Tasks {
			if idx := strings.Index(task.Name, text); idx >= 0 {
				sm[structs.Tasks] = append(sm[structs.Tasks], score(task.Name, ns, idx, job, group.Name))
			}

			// job|group|task|service.name
			for _, service := range task.Services {
				if idx := strings.Index(service.Name, text); idx >= 0 {
					sm[structs.Services] = append(sm[structs.Services], score(service.Name, ns, idx, job, group.Name, task.Name))
				}
			}

			// job|group|task|config.{image,driver,class}
			switch task.Driver {
			case "docker":
				image := getConfigParam(task.Config, "image")
				if idx := strings.Index(image, text); idx >= 0 {
					sm[structs.Images] = append(sm[structs.Images], score(image, ns, idx, job, group.Name, task.Name))
				}
			case "exec", "raw_exec":
				command := getConfigParam(task.Config, "command")
				if idx := strings.Index(command, text); idx >= 0 {
					sm[structs.Commands] = append(sm[structs.Commands], score(command, ns, idx, job, group.Name, task.Name))
				}
			case "java":
				class := getConfigParam(task.Config, "class")
				if idx := strings.Index(class, text); idx >= 0 {
					sm[structs.Classes] = append(sm[structs.Classes], score(class, ns, idx, job, group.Name, task.Name))
				}
			}
		}
	}

	return sm
}

func getConfigParam(config map[string]interface{}, param string) string {
	if config == nil || config[param] == nil {
		return ""
	}

	s, ok := config[param].(string)
	if !ok {
		return ""
	}

	return s
}

type fuzzyMatch struct {
	id    string
	scope []string
	score int
}

func score(id, namespace string, score int, scope ...string) fuzzyMatch {
	return fuzzyMatch{
		id:    id,
		score: score,
		scope: append([]string{namespace}, scope...),
	}
}

func sortSet(text string, matches []fuzzyMatch) {
	sort.Slice(matches, func(a, b int) bool {
		A, B := matches[a], matches[b]

		// sort by index
		switch {
		case A.score < B.score:
			return true
		case B.score < A.score:
			return false
		}

		// shorter length matched text is more likely to be the thing being
		// searched for (in theory)
		//
		// this also causes exact matches to score best, which is desirable
		idA, idB := A.id, B.id
		switch {
		case len(idA) < len(idB):
			return true
		case len(idB) < len(idA):
			return false
		}

		// same index and same length, break ties alphabetically
		return idA < idB
	})
}

// getResourceIter takes a context and returns a memdb iterator specific to
// that context
func getResourceIter(context structs.Context, aclObj *acl.ACL, namespace, prefix string, ws memdb.WatchSet, state *state.StateStore) (memdb.ResultIterator, error) {
	fmt.Println("getResourceIter, context:", context)

	switch context {
	case structs.Jobs:
		return state.JobsByIDPrefix(ws, namespace, prefix)
	case structs.Evals:
		return state.EvalsByIDPrefix(ws, namespace, prefix)
	case structs.Allocs:
		return state.AllocsByIDPrefix(ws, namespace, prefix)
	case structs.Nodes:
		return state.NodesByIDPrefix(ws, prefix)
	case structs.Deployments:
		return state.DeploymentsByIDPrefix(ws, namespace, prefix)
	case structs.Plugins:
		return state.CSIPluginsByIDPrefix(ws, prefix)
	case structs.ScalingPolicies:
		return state.ScalingPoliciesByIDPrefix(ws, namespace, prefix)
	case structs.Volumes:
		return state.CSIVolumesByIDPrefix(ws, namespace, prefix)
	case structs.Namespaces:
		iter, err := state.NamespacesByNamePrefix(ws, prefix)
		if err != nil {
			return nil, err
		}
		if aclObj == nil {
			return iter, nil
		}
		return memdb.NewFilterIterator(iter, namespaceFilter(aclObj)), nil
	default:
		return getEnterpriseResourceIter(context, aclObj, namespace, prefix, ws, state)
	}
}

// namespaceFilter wraps a namespace iterator with a filter for removing
// namespaces the ACL can't access.
func namespaceFilter(aclObj *acl.ACL) memdb.FilterFunc {
	return func(v interface{}) bool {
		return !aclObj.AllowNamespace(v.(*structs.Namespace).Name)
	}
}

// If the length of a prefix is odd, return a subset to the last even character
// This only applies to UUIDs, jobs are excluded
func roundUUIDDownIfOdd(prefix string, context structs.Context) string {
	if context == structs.Jobs {
		return prefix
	}

	// We ignore the count of hyphens when calculating if the prefix is even:
	// E.g "e3671fa4-21"
	numHyphens := strings.Count(prefix, "-")
	l := len(prefix) - numHyphens
	if l%2 == 0 {
		return prefix
	}
	return prefix[:len(prefix)-1]
}

// silenceError determines whether err is an error we care about when getting an
// iterator from the state store - we ignore errors about invalid UUIDs, since
// we sometimes try to lookup by Name and not UUID.
func (*Search) silenceError(err error) bool {
	if err == nil {
		return true
	}

	e := err.Error()
	switch {
	// Searching other contexts with job names raises an error, which in
	// this case we want to ignore.
	case strings.Contains(e, "Invalid UUID: encoding/hex"):
	case strings.Contains(e, "UUID have 36 characters"):
	case strings.Contains(e, "must be even length"):
	case strings.Contains(e, "UUID should have maximum of 4"):
	default:
		// err was not nil and not about UUID prefix, something bad happened
		return false
	}

	return true
}

// PrefixSearch is used to list matches for a given prefix, and returns
// matching jobs, evaluations, allocations, and/or nodes.
func (s *Search) PrefixSearch(args *structs.SearchRequest, reply *structs.SearchResponse) error {
	if done, err := s.srv.forward("Search.PrefixSearch", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"nomad", "search", "prefix_search"}, time.Now())

	aclObj, err := s.srv.ResolveToken(args.AuthToken)
	if err != nil {
		return err
	}

	namespace := args.RequestNamespace()

	// Require either node:read or namespace:read-job
	if !sufficientSearchPerms(aclObj, namespace, args.Context) {
		return structs.ErrPermissionDenied
	}

	reply.Matches = make(map[structs.Context][]string)
	reply.Truncations = make(map[structs.Context]bool)

	// Setup the blocking query
	opts := blockingOptions{
		queryMeta: &reply.QueryMeta,
		queryOpts: &structs.QueryOptions{},
		run: func(ws memdb.WatchSet, state *state.StateStore) error {

			iters := make(map[structs.Context]memdb.ResultIterator)

			contexts := expandSearchContexts(aclObj, namespace, args.Context)
			fmt.Println("CONTEXTS:", contexts)

			for _, ctx := range contexts {
				iter, err := getResourceIter(ctx, aclObj, namespace, roundUUIDDownIfOdd(args.Prefix, args.Context), ws, state)
				fmt.Println("getRI ctx:", ctx, "prefix:", args.Prefix, "aCtx:", args.Context, "iter:", iter, "err:", err)
				if err != nil {
					if !s.silenceError(err) {
						return err
					}
				} else {
					fmt.Println("set context:", ctx, "iter:", iter)
					iters[ctx] = iter
				}
			}

			// Return matches for the given prefix
			for k, v := range iters {
				res, isTrunc := s.getPrefixMatches(v, args.Prefix)
				reply.Matches[k] = res
				reply.Truncations[k] = isTrunc
			}

			// Set the index for the context. If the context has been specified, it
			// will be used as the index of the response. Otherwise, the
			// maximum index from all resources will be used.
			for _, ctx := range contexts {
				index, err := state.Index(contextToIndex(ctx))
				if err != nil {
					return err
				}
				if index > reply.Index {
					reply.Index = index
				}
			}

			s.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		}}
	return s.srv.blockingRPC(&opts)
}

// FuzzySearch is used to list fuzzy or prefix matches for a given text argument and Context.
// If the Context is "all", all searchable contexts are searched. If ACLs are enabled,
// results are limited to policies of the provided ACL token.
//
// These types are limited to prefix UUID searching:
//   Evals, Deployments, ScalingPolicies, Volumes
//
// These types are available for fuzzy searching:
//   Nodes, Namespaces, Jobs, Allocs, Plugins
//
// Jobs are a special case that expand into multiple types, and whose return
// values include Scope which is a descending list of IDs of parent objects,
// starting with the Namespace. The subtypes of jobs are fuzzy searchable.
//
// The Jobs type expands into these sub types:
//   Jobs, Groups, Services, Tasks, Images, Commands, Classes
//
// The results are in descending order starting with strongest match, per Context type.
func (s *Search) FuzzySearch(args *structs.FuzzySearchRequest, reply *structs.FuzzySearchResponse) error {
	if done, err := s.srv.forward("Search.FuzzySearch", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"nomad", "search", "fuzzy_search"}, time.Now())

	min := s.srv.config.SearchConfig.MinTermLength
	fmt.Println("min:", min, "len:", len(args.Text))

	if n := len(args.Text); n < min {
		return fmt.Errorf("fuzzy search query must be at least %d characters, got %d", min, n)
	}

	aclObj, err := s.srv.ResolveToken(args.AuthToken)
	if err != nil {
		return err
	}

	namespace := args.RequestNamespace()
	context := args.Context

	if !sufficientSearchPerms(aclObj, namespace, context) {
		return structs.ErrPermissionDenied
	}

	reply.Matches = make(map[structs.Context][]structs.FuzzyMatch)
	reply.Truncations = make(map[structs.Context]bool)

	// Setup the blocking query
	opts := blockingOptions{
		queryMeta: &reply.QueryMeta,
		queryOpts: new(structs.QueryOptions),
		run: func(ws memdb.WatchSet, state *state.StateStore) error {

			fuzzyIters := make(map[structs.Context]memdb.ResultIterator)
			prefixIters := make(map[structs.Context]memdb.ResultIterator)
			contexts := expandSearchContexts(aclObj, namespace, context)

			for _, ctx := range contexts {
				switch ctx {
				// types that use UUID prefix searching
				case structs.Evals, structs.Deployments, structs.ScalingPolicies, structs.Volumes:
					iter, err := getResourceIter(ctx, aclObj, namespace, roundUUIDDownIfOdd(args.Prefix, args.Context), ws, state)
					if err != nil {
						if !s.silenceError(err) {
							return err
						}
					} else {
						prefixIters[ctx] = iter
					}

				// types that use fuzzy searching
				default:
					iter, err := getResourceIter(ctx, aclObj, namespace, "", ws, state)
					if err != nil {
						return err
					}
					fuzzyIters[ctx] = iter
				}
			}

			// Set prefix matches of the given text
			for ctx, iter := range prefixIters {
				res, isTrunc := s.getPrefixMatches(iter, args.Text)
				matches := make([]structs.FuzzyMatch, 0, len(res))
				for _, result := range res {
					matches = append(matches, structs.FuzzyMatch{ID: result})
				}
				reply.Matches[ctx] = matches
				reply.Truncations[ctx] = isTrunc
			}

			// Set fuzzy matches of the given text
			for _, iter := range fuzzyIters {
				matches, truncations := s.getFuzzyMatches(iter, args.Text)
				for ctx := range matches {
					reply.Matches[ctx] = matches[ctx]
				}
				for ctx := range truncations {
					reply.Truncations[ctx] = truncations[ctx]
				}
			}

			// Set the index for the context. If the context has been specified,
			// it will be used as the index of the response. Otherwise, the maximum
			// index from all the resources will be used.
			for _, ctx := range contexts {
				index, err := state.Index(contextToIndex(ctx))
				if err != nil {
					return err
				}
				if index > reply.Index {
					reply.Index = index
				}
			}

			s.srv.setQueryMeta(&reply.QueryMeta)
			return nil
		},
	}

	return s.srv.blockingRPC(&opts)
}

func expandContext(context structs.Context) []structs.Context {
	switch context {
	case structs.All:
		c := make([]structs.Context, len(allContexts))
		copy(c, allContexts)
		return c
	default:
		return []structs.Context{context}
	}
}
