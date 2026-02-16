package controller

const (
	// Namespace where the operator config ConfigMap lives
	defaultConfigNs = "mec-system"

	// ConfigMap name used by the operator
	defaultConfigMapName = "mec-operator-config"

	// RSU label key used for pinning
	rsuLabelKey = "mec.atnog.org/rsu"
)
