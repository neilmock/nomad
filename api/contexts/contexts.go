package contexts

// Context defines the scope in which a search for Nomad object operates
type Context string

const (
	// These Context types are used to reference the high level Nomad object
	// types than can be searched.
	Allocs          Context = "allocs"
	Deployments     Context = "deployment"
	Evals           Context = "evals"
	Jobs            Context = "jobs"
	Nodes           Context = "nodes"
	Namespaces      Context = "namespaces"
	Quotas          Context = "quotas"
	Recommendations Context = "recommendations"
	ScalingPolicies Context = "scaling_policy"
	Plugins         Context = "plugins"
	Volumes         Context = "volumes"

	// These Context types are used to associate a search result from a lower
	// level Nomad object with one of the higher level Context types above.
	Groups   Context = "groups"
	Tasks    Context = "tasks"
	Images   Context = "images"
	Services Context = "services"

	// Meta Context used to represent the set of all the higher level Context types.
	All Context = "all"
)
