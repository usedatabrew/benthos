package mock_test

import (
	"github.com/usedatabrew/benthos/v4/internal/component/input"
	"github.com/usedatabrew/benthos/v4/internal/manager/mock"
)

var _ input.Streamed = &mock.Input{}
