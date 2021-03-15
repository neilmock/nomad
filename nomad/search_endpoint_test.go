package nomad

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	msgpackrpc "github.com/hashicorp/net-rpc-msgpackrpc"
	"github.com/hashicorp/nomad/acl"
	"github.com/hashicorp/nomad/helper/uuid"
	"github.com/hashicorp/nomad/nomad/mock"
	"github.com/hashicorp/nomad/nomad/state"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/hashicorp/nomad/testutil"
	"github.com/stretchr/testify/require"
)

const jobIndex = 1000

func registerMockJob(s *Server, t *testing.T, prefix string, counter int) *structs.Job {
	job := mock.Job()
	job.ID = prefix + strconv.Itoa(counter)
	registerJob(s, t, job)
	return job
}

func registerJob(s *Server, t *testing.T, job *structs.Job) {
	fsmState := s.fsm.State()
	require.NoError(t, fsmState.UpsertJob(structs.MsgTypeTestSetup, jobIndex, job))
}

func TestSearch_PrefixSearch_Job(t *testing.T) {
	t.Parallel()

	prefix := "aaaaaaaa-e8f7-fd38-c855-ab94ceb8970"

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)

	job := registerMockJob(s, t, prefix, 0)

	req := &structs.SearchRequest{
		Prefix:  prefix,
		Context: structs.Jobs,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: job.Namespace,
		},
	}

	var resp structs.SearchResponse
	if err := msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp); err != nil {
		t.Fatalf("err: %v", err)
	}

	require.Len(t, resp.Matches[structs.Jobs], 1)
	require.Equal(t, job.ID, resp.Matches[structs.Jobs][0])
	require.Equal(t, uint64(jobIndex), resp.Index)
}

func TestSearch_PrefixSearch_ACL(t *testing.T) {
	t.Parallel()

	jobID := "aaaaaaaa-e8f7-fd38-c855-ab94ceb8970"

	s, root, cleanupS := TestACLServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)
	fsmState := s.fsm.State()

	job := registerMockJob(s, t, jobID, 0)
	require.NoError(t, fsmState.UpsertNode(structs.MsgTypeTestSetup, 1001, mock.Node()))

	req := &structs.SearchRequest{
		Prefix:  "",
		Context: structs.Jobs,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: job.Namespace,
		},
	}

	// Try without a token and expect failure
	{
		var resp structs.SearchResponse
		err := msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp)
		require.EqualError(t, err, structs.ErrPermissionDenied.Error())
	}

	// Try with an invalid token and expect failure
	{
		invalidToken := mock.CreatePolicyAndToken(t, fsmState, 1003, "test-invalid",
			mock.NamespacePolicy(structs.DefaultNamespace, "", []string{acl.NamespaceCapabilityListJobs}))
		req.AuthToken = invalidToken.SecretID
		var resp structs.SearchResponse
		err := msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp)
		require.EqualError(t, err, structs.ErrPermissionDenied.Error())
	}

	// Try with a node:read token and expect failure due to Jobs being the context
	{
		validToken := mock.CreatePolicyAndToken(t, fsmState, 1005, "test-invalid2", mock.NodePolicy(acl.PolicyRead))
		req.AuthToken = validToken.SecretID
		var resp structs.SearchResponse
		err := msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp)
		require.EqualError(t, err, structs.ErrPermissionDenied.Error())
	}

	// Try with a node:read token and expect success due to All context
	{
		validToken := mock.CreatePolicyAndToken(t, fsmState, 1007, "test-valid", mock.NodePolicy(acl.PolicyRead))
		req.Context = structs.All
		req.AuthToken = validToken.SecretID
		var resp structs.SearchResponse
		require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))
		require.Equal(t, uint64(1001), resp.Index)
		require.Len(t, resp.Matches[structs.Nodes], 1)

		// Jobs filtered out since token only has access to node:read
		require.Len(t, resp.Matches[structs.Jobs], 0)
	}

	// Try with a valid token for namespace:read-job
	{
		validToken := mock.CreatePolicyAndToken(t, fsmState, 1009, "test-valid2",
			mock.NamespacePolicy(structs.DefaultNamespace, "", []string{acl.NamespaceCapabilityReadJob}))
		req.AuthToken = validToken.SecretID
		var resp structs.SearchResponse
		require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))
		require.Len(t, resp.Matches[structs.Jobs], 1)
		require.Equal(t, job.ID, resp.Matches[structs.Jobs][0])

		// Index of job - not node - because node context is filtered out
		require.Equal(t, uint64(1000), resp.Index)

		// Nodes filtered out since token only has access to namespace:read-job
		require.Len(t, resp.Matches[structs.Nodes], 0)
	}

	// Try with a valid token for node:read and namespace:read-job
	{
		validToken := mock.CreatePolicyAndToken(t, fsmState, 1011, "test-valid3", strings.Join([]string{
			mock.NamespacePolicy(structs.DefaultNamespace, "", []string{acl.NamespaceCapabilityReadJob}),
			mock.NodePolicy(acl.PolicyRead),
		}, "\n"))
		req.AuthToken = validToken.SecretID
		var resp structs.SearchResponse
		require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))
		require.Len(t, resp.Matches[structs.Jobs], 1)
		require.Equal(t, job.ID, resp.Matches[structs.Jobs][0])
		require.Len(t, resp.Matches[structs.Nodes], 1)
		require.Equal(t, uint64(1001), resp.Index)
	}

	// Try with a management token
	{
		req.AuthToken = root.SecretID
		var resp structs.SearchResponse
		require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))
		require.Equal(t, uint64(1001), resp.Index)
		require.Len(t, resp.Matches[structs.Jobs], 1)
		require.Equal(t, job.ID, resp.Matches[structs.Jobs][0])
		require.Len(t, resp.Matches[structs.Nodes], 1)
	}
}

func TestSearch_PrefixSearch_All_JobWithHyphen(t *testing.T) {
	t.Parallel()

	prefix := "example-test-------" // Assert that a job with more than 4 hyphens works

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)

	// Register a job and an allocation
	job := registerMockJob(s, t, prefix, 0)
	alloc := mock.Alloc()
	alloc.JobID = job.ID
	alloc.Namespace = job.Namespace
	summary := mock.JobSummary(alloc.JobID)
	fsmState := s.fsm.State()

	require.NoError(t, fsmState.UpsertJobSummary(999, summary))
	require.NoError(t, fsmState.UpsertAllocs(structs.MsgTypeTestSetup, 1000, []*structs.Allocation{alloc}))

	req := &structs.SearchRequest{
		Context: structs.All,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: job.Namespace,
		},
	}

	// req.Prefix = "example-te": 9
	for i := 1; i < len(prefix); i++ {
		req.Prefix = prefix[:i]
		var resp structs.SearchResponse
		require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))
		require.Equal(t, 1, len(resp.Matches[structs.Jobs]))
		require.Equal(t, job.ID, resp.Matches[structs.Jobs][0])
		require.EqualValues(t, jobIndex, resp.Index)
	}
}

func TestSearch_PrefixSearch_All_LongJob(t *testing.T) {
	t.Parallel()

	prefix := strings.Repeat("a", 100)

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)

	// Register a job and an allocation
	job := registerMockJob(s, t, prefix, 0)
	alloc := mock.Alloc()
	alloc.JobID = job.ID
	summary := mock.JobSummary(alloc.JobID)
	fsmState := s.fsm.State()

	require.NoError(t, fsmState.UpsertJobSummary(999, summary))
	require.NoError(t, fsmState.UpsertAllocs(structs.MsgTypeTestSetup, 1000, []*structs.Allocation{alloc}))

	req := &structs.SearchRequest{
		Prefix:  prefix,
		Context: structs.All,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: job.Namespace,
		},
	}

	var resp structs.SearchResponse
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))

	require.Len(t, resp.Matches[structs.Jobs], 1)
	require.Equal(t, job.ID, resp.Matches[structs.Jobs][0])
	require.EqualValues(t, jobIndex, resp.Index)
}

// truncate should limit results to 20
func TestSearch_PrefixSearch_Truncate(t *testing.T) {
	t.Parallel()

	prefix := "aaaaaaaa-e8f7-fd38-c855-ab94ceb8970"

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)

	for counter := 0; counter < 25; counter++ {
		registerMockJob(s, t, prefix, counter)
	}

	req := &structs.SearchRequest{
		Prefix:  prefix,
		Context: structs.Jobs,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: "default",
		},
	}

	var resp structs.SearchResponse
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))

	require.Len(t, resp.Matches[structs.Jobs], 20)
	require.True(t, resp.Truncations[structs.Jobs])
	require.Equal(t, uint64(jobIndex), resp.Index)
}

func TestSearch_PrefixSearch_AllWithJob(t *testing.T) {
	t.Parallel()

	prefix := "aaaaaaaa-e8f7-fd38-c855-ab94ceb8970"

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})

	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)

	job := registerMockJob(s, t, prefix, 0)
	eval1 := mock.Eval()
	eval1.ID = job.ID
	require.NoError(t, s.fsm.State().UpsertEvals(structs.MsgTypeTestSetup, 2000, []*structs.Evaluation{eval1}))

	req := &structs.SearchRequest{
		Prefix:  prefix,
		Context: structs.All,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: job.Namespace,
		},
	}

	var resp structs.SearchResponse
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))

	require.Len(t, resp.Matches[structs.Jobs], 1)
	require.Equal(t, job.ID, resp.Matches[structs.Jobs][0])
	require.Len(t, resp.Matches[structs.Evals], 1)
	require.Equal(t, eval1.ID, resp.Matches[structs.Evals][0])
}

func TestSearch_PrefixSearch_Evals(t *testing.T) {
	t.Parallel()

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)

	eval1 := mock.Eval()
	require.NoError(t, s.fsm.State().UpsertEvals(structs.MsgTypeTestSetup, 2000, []*structs.Evaluation{eval1}))

	prefix := eval1.ID[:len(eval1.ID)-2]

	req := &structs.SearchRequest{
		Prefix:  prefix,
		Context: structs.Evals,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: eval1.Namespace,
		},
	}

	var resp structs.SearchResponse
	if err := msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp); err != nil {
		t.Fatalf("err: %v", err)
	}

	require.Len(t, resp.Matches[structs.Evals], 1)
	require.Equal(t, eval1.ID, resp.Matches[structs.Evals][0])
	require.False(t, resp.Truncations[structs.Evals])
	require.Equal(t, uint64(2000), resp.Index)
}

func TestSearch_PrefixSearch_Allocation(t *testing.T) {
	t.Parallel()

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)

	alloc := mock.Alloc()
	summary := mock.JobSummary(alloc.JobID)
	fsmState := s.fsm.State()

	require.NoError(t, fsmState.UpsertJobSummary(999, summary))
	require.NoError(t, fsmState.UpsertAllocs(structs.MsgTypeTestSetup, 90, []*structs.Allocation{alloc}))

	prefix := alloc.ID[:len(alloc.ID)-2]

	req := &structs.SearchRequest{
		Prefix:  prefix,
		Context: structs.Allocs,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: alloc.Namespace,
		},
	}

	var resp structs.SearchResponse
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))

	require.Len(t, resp.Matches[structs.Allocs], 1)
	require.Equal(t, alloc.ID, resp.Matches[structs.Allocs][0])
	require.False(t, resp.Truncations[structs.Allocs])
	require.Equal(t, uint64(90), resp.Index)
}

func TestSearch_PrefixSearch_All_UUID(t *testing.T) {
	t.Parallel()

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)

	alloc := mock.Alloc()
	summary := mock.JobSummary(alloc.JobID)
	fsmState := s.fsm.State()

	require.NoError(t, fsmState.UpsertJobSummary(999, summary))
	require.NoError(t, fsmState.UpsertAllocs(structs.MsgTypeTestSetup, 1000, []*structs.Allocation{alloc}))

	node := mock.Node()
	if err := fsmState.UpsertNode(structs.MsgTypeTestSetup, 1001, node); err != nil {
		t.Fatalf("err: %v", err)
	}

	eval1 := mock.Eval()
	eval1.ID = node.ID
	if err := fsmState.UpsertEvals(structs.MsgTypeTestSetup, 1002, []*structs.Evaluation{eval1}); err != nil {
		t.Fatalf("err: %v", err)
	}

	req := &structs.SearchRequest{
		Context: structs.All,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: eval1.Namespace,
		},
	}

	for i := 1; i < len(alloc.ID); i++ {
		req.Prefix = alloc.ID[:i]
		var resp structs.SearchResponse
		require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))
		require.Len(t, resp.Matches[structs.Allocs], 1)
		require.Equal(t, alloc.ID, resp.Matches[structs.Allocs][0])
		require.False(t, resp.Truncations[structs.Allocs])
		require.EqualValues(t, 1002, resp.Index)
	}
}

func TestSearch_PrefixSearch_Node(t *testing.T) {
	t.Parallel()

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)

	fsmState := s.fsm.State()
	node := mock.Node()

	require.NoError(t, fsmState.UpsertNode(structs.MsgTypeTestSetup, 100, node))

	prefix := node.ID[:len(node.ID)-2]

	req := &structs.SearchRequest{
		Prefix:  prefix,
		Context: structs.Nodes,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: structs.DefaultNamespace,
		},
	}

	var resp structs.SearchResponse
	if err := msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp); err != nil {
		t.Fatalf("err: %v", err)
	}

	require.Len(t, resp.Matches[structs.Nodes], 1)
	require.Equal(t, node.ID, resp.Matches[structs.Nodes][0])
	require.False(t, resp.Truncations[structs.Nodes])
	require.Equal(t, uint64(100), resp.Index)
}

func TestSearch_PrefixSearch_Deployment(t *testing.T) {
	t.Parallel()

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)

	deployment := mock.Deployment()
	require.NoError(t, s.fsm.State().UpsertDeployment(2000, deployment))

	prefix := deployment.ID[:len(deployment.ID)-2]

	req := &structs.SearchRequest{
		Prefix:  prefix,
		Context: structs.Deployments,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: deployment.Namespace,
		},
	}

	var resp structs.SearchResponse
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))
	require.Len(t, resp.Matches[structs.Deployments], 1)
	require.Equal(t, deployment.ID, resp.Matches[structs.Deployments][0])
	require.False(t, resp.Truncations[structs.Deployments])
	require.Equal(t, uint64(2000), resp.Index)
}

func TestSearch_PrefixSearch_AllContext(t *testing.T) {
	t.Parallel()

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})

	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)

	fsmState := s.fsm.State()
	node := mock.Node()

	require.NoError(t, fsmState.UpsertNode(structs.MsgTypeTestSetup, 100, node))

	eval1 := mock.Eval()
	eval1.ID = node.ID
	require.NoError(t, fsmState.UpsertEvals(structs.MsgTypeTestSetup, 1000, []*structs.Evaluation{eval1}))

	prefix := node.ID[:len(node.ID)-2]

	req := &structs.SearchRequest{
		Prefix:  prefix,
		Context: structs.All,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: eval1.Namespace,
		},
	}

	var resp structs.SearchResponse
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))

	require.Len(t, resp.Matches[structs.Nodes], 1)
	require.Len(t, resp.Matches[structs.Evals], 1)
	require.Equal(t, node.ID, resp.Matches[structs.Nodes][0])
	require.Equal(t, eval1.ID, resp.Matches[structs.Evals][0])
	require.Equal(t, uint64(1000), resp.Index)
}

// Tests that the top 20 matches are returned when no prefix is set
func TestSearch_PrefixSearch_NoPrefix(t *testing.T) {
	t.Parallel()

	prefix := "aaaaaaaa-e8f7-fd38-c855-ab94ceb8970"

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)

	job := registerMockJob(s, t, prefix, 0)

	req := &structs.SearchRequest{
		Prefix:  "",
		Context: structs.Jobs,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: job.Namespace,
		},
	}

	var resp structs.SearchResponse
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))
	require.Len(t, resp.Matches[structs.Jobs], 1)
	require.Equal(t, job.ID, resp.Matches[structs.Jobs][0])
	require.Equal(t, uint64(jobIndex), resp.Index)
}

// Tests that the zero matches are returned when a prefix has no matching
// results
func TestSearch_PrefixSearch_NoMatches(t *testing.T) {
	t.Parallel()

	prefix := "aaaaaaaa-e8f7-fd38-c855-ab94ceb8970"

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)

	req := &structs.SearchRequest{
		Prefix:  prefix,
		Context: structs.Jobs,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: structs.DefaultNamespace,
		},
	}

	var resp structs.SearchResponse
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))
	require.Empty(t, resp.Matches[structs.Jobs])
	require.Equal(t, uint64(0), resp.Index)
}

// Prefixes can only be looked up if their length is a power of two. For
// prefixes which are an odd length, use the length-1 characters.
func TestSearch_PrefixSearch_RoundDownToEven(t *testing.T) {
	t.Parallel()

	id1 := "aaafaaaa-e8f7-fd38-c855-ab94ceb89"
	id2 := "aaafeaaa-e8f7-fd38-c855-ab94ceb89"
	prefix := "aaafa"

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)

	job := registerMockJob(s, t, id1, 0)
	registerMockJob(s, t, id2, 50)

	req := &structs.SearchRequest{
		Prefix:  prefix,
		Context: structs.Jobs,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: job.Namespace,
		},
	}

	var resp structs.SearchResponse
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))
	require.Len(t, resp.Matches[structs.Jobs], 1)
	require.Equal(t, job.ID, resp.Matches[structs.Jobs][0])
}

func TestSearch_PrefixSearch_MultiRegion(t *testing.T) {
	t.Parallel()

	jobName := "exampleexample"

	s1, cleanupS1 := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
		c.Region = "foo"
	})
	defer cleanupS1()

	s2, cleanupS2 := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
		c.Region = "bar"
	})
	defer cleanupS2()

	TestJoin(t, s1, s2)
	testutil.WaitForLeader(t, s1.RPC)

	job := registerMockJob(s1, t, jobName, 0)

	req := &structs.SearchRequest{
		Prefix:  "",
		Context: structs.Jobs,
		QueryOptions: structs.QueryOptions{
			Region:    "foo",
			Namespace: job.Namespace,
		},
	}

	codec := rpcClient(t, s2)

	var resp structs.SearchResponse
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))

	require.Len(t, resp.Matches[structs.Jobs], 1)
	require.Equal(t, job.ID, resp.Matches[structs.Jobs][0])
	require.Equal(t, uint64(jobIndex), resp.Index)
}

func TestSearch_PrefixSearch_CSIPlugin(t *testing.T) {
	t.Parallel()

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)

	id := uuid.Generate()
	state.CreateTestCSIPlugin(s.fsm.State(), id)

	prefix := id[:len(id)-2]

	req := &structs.SearchRequest{
		Prefix:  prefix,
		Context: structs.Plugins,
		QueryOptions: structs.QueryOptions{
			Region: "global",
		},
	}

	var resp structs.SearchResponse
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))

	require.Len(t, resp.Matches[structs.Plugins], 1)
	require.Equal(t, id, resp.Matches[structs.Plugins][0])
	require.False(t, resp.Truncations[structs.Plugins])
}

func TestSearch_PrefixSearch_CSIVolume(t *testing.T) {
	t.Parallel()

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)

	id := uuid.Generate()
	err := s.fsm.State().CSIVolumeRegister(1000, []*structs.CSIVolume{{
		ID:        id,
		Namespace: structs.DefaultNamespace,
		PluginID:  "glade",
	}})
	require.NoError(t, err)

	prefix := id[:len(id)-2]

	req := &structs.SearchRequest{
		Prefix:  prefix,
		Context: structs.Volumes,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: structs.DefaultNamespace,
		},
	}

	var resp structs.SearchResponse
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))

	require.Len(t, resp.Matches[structs.Volumes], 1)
	require.Equal(t, id, resp.Matches[structs.Volumes][0])
	require.False(t, resp.Truncations[structs.Volumes])
}

func TestSearch_PrefixSearch_Namespace(t *testing.T) {
	t.Parallel()

	s, cleanup := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})

	defer cleanup()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)

	ns := mock.Namespace()
	require.NoError(t, s.fsm.State().UpsertNamespaces(2000, []*structs.Namespace{ns}))

	prefix := ns.Name[:len(ns.Name)-2]

	req := &structs.SearchRequest{
		Prefix:  prefix,
		Context: structs.Namespaces,
		QueryOptions: structs.QueryOptions{
			Region: "global",
		},
	}

	var resp structs.SearchResponse
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))

	require.Len(t, resp.Matches[structs.Namespaces], 1)
	require.Equal(t, ns.Name, resp.Matches[structs.Namespaces][0])
	require.False(t, resp.Truncations[structs.Namespaces])
	require.Equal(t, uint64(2000), resp.Index)
}

func TestSearch_PrefixSearch_Namespace_ACL(t *testing.T) {
	t.Parallel()

	s, root, cleanup := TestACLServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})
	defer cleanup()

	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)
	fsmState := s.fsm.State()

	ns := mock.Namespace()
	require.NoError(t, fsmState.UpsertNamespaces(500, []*structs.Namespace{ns}))

	job1 := mock.Job()
	require.NoError(t, fsmState.UpsertJob(structs.MsgTypeTestSetup, 502, job1))

	job2 := mock.Job()
	job2.Namespace = ns.Name
	require.NoError(t, fsmState.UpsertJob(structs.MsgTypeTestSetup, 504, job2))

	require.NoError(t, fsmState.UpsertNode(structs.MsgTypeTestSetup, 1001, mock.Node()))

	req := &structs.SearchRequest{
		Prefix:  "",
		Context: structs.Jobs,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: job1.Namespace,
		},
	}

	// Try without a token and expect failure
	{
		var resp structs.SearchResponse
		err := msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp)
		require.EqualError(t, err, structs.ErrPermissionDenied.Error())
	}

	// Try with an invalid token and expect failure
	{
		invalidToken := mock.CreatePolicyAndToken(t, fsmState, 1003, "test-invalid",
			mock.NamespacePolicy(structs.DefaultNamespace, "", []string{acl.NamespaceCapabilityListJobs}))
		req.AuthToken = invalidToken.SecretID
		var resp structs.SearchResponse
		err := msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp)
		require.EqualError(t, err, structs.ErrPermissionDenied.Error())
	}

	// Try with a node:read token and expect failure due to Namespaces being the context
	{
		validToken := mock.CreatePolicyAndToken(t, fsmState, 1005, "test-invalid2", mock.NodePolicy(acl.PolicyRead))
		req.Context = structs.Namespaces
		req.AuthToken = validToken.SecretID
		var resp structs.SearchResponse
		err := msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp)
		require.EqualError(t, err, structs.ErrPermissionDenied.Error())
	}

	// Try with a node:read token and expect success due to All context
	{
		validToken := mock.CreatePolicyAndToken(t, fsmState, 1007, "test-valid", mock.NodePolicy(acl.PolicyRead))
		req.Context = structs.All
		req.AuthToken = validToken.SecretID
		var resp structs.SearchResponse
		require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))
		require.Equal(t, uint64(1001), resp.Index)
		require.Len(t, resp.Matches[structs.Nodes], 1)

		// Jobs filtered out since token only has access to node:read
		require.Len(t, resp.Matches[structs.Jobs], 0)
	}

	// Try with a valid token for non-default namespace:read-job
	{
		validToken := mock.CreatePolicyAndToken(t, fsmState, 1009, "test-valid2",
			mock.NamespacePolicy(job2.Namespace, "", []string{acl.NamespaceCapabilityReadJob}))
		req.Context = structs.All
		req.AuthToken = validToken.SecretID
		req.Namespace = job2.Namespace
		var resp structs.SearchResponse
		require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))
		require.Len(t, resp.Matches[structs.Jobs], 1)
		require.Equal(t, job2.ID, resp.Matches[structs.Jobs][0])
		require.Len(t, resp.Matches[structs.Namespaces], 1)

		// Index of job - not node - because node context is filtered out
		require.Equal(t, uint64(504), resp.Index)

		// Nodes filtered out since token only has access to namespace:read-job
		require.Len(t, resp.Matches[structs.Nodes], 0)
	}

	// Try with a valid token for node:read and default namespace:read-job
	{
		validToken := mock.CreatePolicyAndToken(t, fsmState, 1011, "test-valid3", strings.Join([]string{
			mock.NamespacePolicy(structs.DefaultNamespace, "", []string{acl.NamespaceCapabilityReadJob}),
			mock.NodePolicy(acl.PolicyRead),
		}, "\n"))
		req.Context = structs.All
		req.AuthToken = validToken.SecretID
		req.Namespace = structs.DefaultNamespace
		var resp structs.SearchResponse
		require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))
		require.Len(t, resp.Matches[structs.Jobs], 1)
		require.Equal(t, job1.ID, resp.Matches[structs.Jobs][0])
		require.Len(t, resp.Matches[structs.Nodes], 1)
		require.Equal(t, uint64(1001), resp.Index)
		require.Len(t, resp.Matches[structs.Namespaces], 1)
	}

	// Try with a management token
	{
		req.Context = structs.All
		req.AuthToken = root.SecretID
		req.Namespace = structs.DefaultNamespace
		var resp structs.SearchResponse
		require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))
		require.Equal(t, uint64(1001), resp.Index)
		require.Len(t, resp.Matches[structs.Jobs], 1)
		require.Equal(t, job1.ID, resp.Matches[structs.Jobs][0])
		require.Len(t, resp.Matches[structs.Nodes], 1)
		require.Len(t, resp.Matches[structs.Namespaces], 2)
	}
}

func TestSearch_PrefixSearch_ScalingPolicy(t *testing.T) {
	t.Parallel()

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)

	job, policy := mock.JobWithScalingPolicy()
	prefix := policy.ID
	fsmState := s.fsm.State()

	require.NoError(t, fsmState.UpsertJob(structs.MsgTypeTestSetup, jobIndex, job))

	req := &structs.SearchRequest{
		Prefix:  prefix,
		Context: structs.ScalingPolicies,
		QueryOptions: structs.QueryOptions{
			Region:    "global",
			Namespace: job.Namespace,
		},
	}

	var resp structs.SearchResponse
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))
	require.Len(t, resp.Matches[structs.ScalingPolicies], 1)
	require.Equal(t, policy.ID, resp.Matches[structs.ScalingPolicies][0])
	require.Equal(t, uint64(jobIndex), resp.Index)

	req.Context = structs.All
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.PrefixSearch", req, &resp))
	require.Len(t, resp.Matches[structs.ScalingPolicies], 1)
	require.Equal(t, policy.ID, resp.Matches[structs.ScalingPolicies][0])
	require.Equal(t, uint64(jobIndex), resp.Index)
}

func TestSearch_FuzzySearch_ACL(t *testing.T) {
	t.Parallel()

	s, root, cleanupS := TestACLServer(t, func(c *Config) {
		c.NumSchedulers = 0
		c.SearchConfig.MinTermLength = 1
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)
	fsmState := s.fsm.State()

	job := mock.Job()
	registerJob(s, t, job)

	require.NoError(t, fsmState.UpsertNode(structs.MsgTypeTestSetup, 1001, mock.Node()))

	req := &structs.FuzzySearchRequest{
		Text:         "set-this-in-test",
		Context:      structs.Jobs,
		QueryOptions: structs.QueryOptions{Region: "global", Namespace: job.Namespace},
	}

	// Try without a token and expect failure
	{
		var resp structs.FuzzySearchResponse
		err := msgpackrpc.CallWithCodec(codec, "Search.FuzzySearch", req, &resp)
		require.EqualError(t, err, structs.ErrPermissionDenied.Error())
	}

	// Try with an invalid token and expect failure
	{
		invalidToken := mock.CreatePolicyAndToken(t, fsmState, 1003, "test-invalid",
			mock.NamespacePolicy(structs.DefaultNamespace, "", []string{acl.NamespaceCapabilityListJobs}))
		req.AuthToken = invalidToken.SecretID
		var resp structs.FuzzySearchResponse
		err := msgpackrpc.CallWithCodec(codec, "Search.FuzzySearch", req, &resp)
		require.EqualError(t, err, structs.ErrPermissionDenied.Error())
	}

	// Try with a node:read token and expect failure due to Jobs being the context
	{
		validToken := mock.CreatePolicyAndToken(t, fsmState, 1005, "test-invalid2", mock.NodePolicy(acl.PolicyRead))
		req.AuthToken = validToken.SecretID
		var resp structs.FuzzySearchResponse
		err := msgpackrpc.CallWithCodec(codec, "Search.FuzzySearch", req, &resp)
		require.EqualError(t, err, structs.ErrPermissionDenied.Error())
	}

	// Try with a node:read token and expect success due to All context
	{
		validToken := mock.CreatePolicyAndToken(t, fsmState, 1007, "test-valid", mock.NodePolicy(acl.PolicyRead))
		req.Context = structs.All
		req.AuthToken = validToken.SecretID
		req.Text = "oo" // mock node ID is foobar
		var resp structs.FuzzySearchResponse
		require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.FuzzySearch", req, &resp))
		require.Equal(t, uint64(1001), resp.Index)
		require.Len(t, resp.Matches[structs.Nodes], 1)

		// Jobs filtered out since token only has access to node:read
		require.Len(t, resp.Matches[structs.Jobs], 0)
	}

	// Try with a valid token for namespace:read-job
	{
		validToken := mock.CreatePolicyAndToken(t, fsmState, 1009, "test-valid2",
			mock.NamespacePolicy(structs.DefaultNamespace, "", []string{acl.NamespaceCapabilityReadJob}))
		req.AuthToken = validToken.SecretID
		req.Text = "jo" // mock job Name is my-job
		var resp structs.FuzzySearchResponse
		require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.FuzzySearch", req, &resp))
		require.Len(t, resp.Matches[structs.Jobs], 1)
		require.Equal(t, structs.FuzzyMatch{
			ID:    "my-job",
			Scope: []string{"default"},
		}, resp.Matches[structs.Jobs][0])

		// Index of job - not node - because node context is filtered out
		require.Equal(t, uint64(1000), resp.Index)

		// Nodes filtered out since token only has access to namespace:read-job
		require.Len(t, resp.Matches[structs.Nodes], 0)
	}

	// Try with a management token
	{
		req.AuthToken = root.SecretID
		var resp structs.FuzzySearchResponse
		req.Text = "o" // matches Job:my-job and Node:foobar
		require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.FuzzySearch", req, &resp))
		require.Equal(t, uint64(1001), resp.Index)
		require.Len(t, resp.Matches[structs.Jobs], 1)
		require.Equal(t, structs.FuzzyMatch{
			ID: "my-job", Scope: []string{"default"},
		}, resp.Matches[structs.Jobs][0])
		require.Len(t, resp.Matches[structs.Nodes], 1)
		require.Equal(t, structs.FuzzyMatch{
			ID: "foobar",
		}, resp.Matches[structs.Nodes][0])
	}
}

func TestSearch_FuzzySearch_ShortText(t *testing.T) {
	t.Parallel()

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
		c.SearchConfig.MinTermLength = 5
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)
	fsmState := s.fsm.State()

	job := mock.Job()
	registerJob(s, t, job)

	require.NoError(t, fsmState.UpsertNode(structs.MsgTypeTestSetup, 1001, mock.Node()))

	req := &structs.FuzzySearchRequest{
		Text:         "foo", // min set to 5
		Context:      structs.Jobs,
		QueryOptions: structs.QueryOptions{Region: "global", Namespace: job.Namespace},
	}

	var resp structs.FuzzySearchResponse
	require.EqualError(t, msgpackrpc.CallWithCodec(codec, "Search.FuzzySearch", req, &resp),
		"fuzzy search query must be at least 5 characters, got 3")
}

func TestSearch_FuzzySearch_Truncate(t *testing.T) {
	t.Parallel()

	s, cleanupS := TestServer(t, func(c *Config) {
		c.NumSchedulers = 0
	})
	defer cleanupS()
	codec := rpcClient(t, s)
	testutil.WaitForLeader(t, s.RPC)
	fsmState := s.fsm.State()

	require.NoError(t, fsmState.UpsertNode(structs.MsgTypeTestSetup, 1001, mock.Node()))

	req := &structs.FuzzySearchRequest{
		Text:         "job",
		Context:      structs.Jobs,
		QueryOptions: structs.QueryOptions{Region: "global", Namespace: "default"},
	}

	for i := 0; i < 25; i++ {
		job := mock.Job()
		job.Name = fmt.Sprintf("my-job-%d", i)
		registerJob(s, t, job)
	}

	var resp structs.FuzzySearchResponse
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Search.FuzzySearch", req, &resp))

	require.Len(t, resp.Matches[structs.Jobs], 20)
	require.True(t, resp.Truncations[structs.Jobs])
	require.Equal(t, uint64(jobIndex), resp.Index)
}

func TestSearch_FuzzySearch_Job(t *testing.T) {
	// todo, the big one
}
