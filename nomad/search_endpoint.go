package nomad

import (
	"fmt"
	"regexp"
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

	fuzzyContexts = []structs.Context{
		structs.Nodes,
		structs.Namespaces,
		structs.Jobs,
		structs.Allocs,
	}
)

// Search endpoint is used to look up matches for a given prefix and context
type Search struct {
	srv    *Server
	logger log.Logger
}

// getMatches extracts matches for an iterator, and returns a list of ids for
// these matches.
func (s *Search) getMatches(iter memdb.ResultIterator, prefix string) ([]string, bool) {
	var matches []string

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

func (s *Search) getFuzzyMatches(iter memdb.ResultIterator, re *regexp.Regexp) ([]string, bool) {
	type match struct {
		value string // the thing matched (e.g. job name)
		pos   int    // the quality of result (lower is better)
	}

	var matches []match

	for i := 0; i < truncateLimit; i++ {
		raw := iter.Next()
		if raw == nil {
			break
		}

		var name string
		switch t := raw.(type) {
		case *structs.Node:
			name = t.Name
		case *structs.Namespace:
			name = t.Name
		case *structs.Job:
			name = t.Name
		case *structs.Allocation:
			name = t.Name
		}

		if m := re.FindStringIndex(name); len(m) > 0 {
			matches = append(matches, match{
				value: name,
				pos:   m[0],
			})
		}
	}

	sort.Slice(matches, func(a, b int) bool {
		A := matches[a]
		B := matches[b]

		switch {
		case A.pos < B.pos:
			return true
		case B.pos < A.pos:
			return false

		case len(A.value) < len(B.value):
			return true
		case len(B.value) < len(A.value):
			return false
		}

		return A.value < B.value
	})

	results := make([]string, 0, len(matches))
	for _, m := range matches {
		results = append(results, m.value)
	}

	return results, iter.Next() != nil
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

			contexts := searchContexts(aclObj, namespace, args.Context)

			for _, ctx := range contexts {
				iter, err := getResourceIter(ctx, aclObj, namespace, roundUUIDDownIfOdd(args.Prefix, args.Context), ws, state)
				if err != nil {
					e := err.Error()
					switch {
					// Searching other contexts with job names raises an error, which in
					// this case we want to ignore.
					case strings.Contains(e, "Invalid UUID: encoding/hex"):
					case strings.Contains(e, "UUID have 36 characters"):
					case strings.Contains(e, "must be even length"):
					case strings.Contains(e, "UUID should have maximum of 4"):
					default:
						return err
					}
				} else {
					iters[ctx] = iter
				}
			}

			// Return matches for the given prefix
			for k, v := range iters {
				res, isTrunc := s.getMatches(v, args.Prefix)
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

// FuzzySearch is used to list fuzzy matches for a given string, and returns matching
// jobs, nodes, namespaces, (etc?).
func (s *Search) FuzzySearch(args *structs.FuzzySearchRequest, reply *structs.SearchResponse) error {
	fmt.Println("FuzzySearch, text:", args.Text)

	if done, err := s.srv.forward("Search.FuzzySearch", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"nomad", "search", "fuzzy_search"}, time.Now())

	aclObj, err := s.srv.ResolveToken(args.AuthToken)
	if err != nil {
		return err
	}

	namespace := args.RequestNamespace()

	if !sufficientSearchPerms(aclObj, namespace, structs.Fuzzy) {
		return structs.ErrPermissionDenied
	}

	reply.Matches = make(map[structs.Context][]string)
	reply.Truncations = make(map[structs.Context]bool)

	// Setup the blocking query
	opts := blockingOptions{
		queryMeta: &reply.QueryMeta,
		queryOpts: new(structs.QueryOptions),
		run: func(ws memdb.WatchSet, state *state.StateStore) error {
			fmt.Println("SH do run in blocking query")

			iters := make(map[structs.Context]memdb.ResultIterator)

			fmt.Println("doSearch, fuzzy:", structs.Fuzzy)
			contexts := searchContexts(aclObj, namespace, structs.Fuzzy)

			for _, ctx := range contexts {
				noPrefix := "" // search everything
				iter, err := getResourceIter(ctx, aclObj, namespace, noPrefix, ws, state)
				if err != nil {
					return err
				}
				iters[ctx] = iter
			}

			// compile the matcher once and reuse it
			re := regexp.MustCompile(args.Text)

			// Return fuzzy matches for the given text
			for k, v := range iters {
				res, isTrunc := s.getFuzzyMatches(v, re)
				reply.Matches[k] = res
				reply.Truncations[k] = isTrunc
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
	fmt.Println("expand:", context)

	switch context {
	case structs.All:
		c := make([]structs.Context, len(allContexts))
		copy(c, allContexts)
		return c
	case structs.Fuzzy:
		c := make([]structs.Context, len(fuzzyContexts))
		copy(c, fuzzyContexts)
		return c
	default:
		return []structs.Context{context}
	}
}
