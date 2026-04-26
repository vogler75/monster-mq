package version

const (
	Version = "0.1.0"
	Name    = "monstermq-edge"
)

func String() string {
	return Name + " " + Version
}
