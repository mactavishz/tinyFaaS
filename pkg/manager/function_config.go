package manager

type FunctionConfig struct {
	Name     string             `json:"name"`
	Env      string             `json:"env"`
	Replicas int                `json:"replicas"`
	Envs     map[string]string  `json:"envs"`
	Labels   map[string]string  `json:"labels"`
	Limits   FunctionResources  `json:"limits"`
	Requests *FunctionResources `json:"requests,omitempty"`

	EffectiveLimits EffectiveLimits `json:"effective_limits"`
	Running         bool            `json:"running"`
}
