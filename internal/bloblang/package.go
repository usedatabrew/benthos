package bloblang

import (
	"github.com/usedatabrew/benthos/v4/internal/bloblang/plugins"
)

func init() {
	if err := plugins.Register(); err != nil {
		panic(err)
	}
}
