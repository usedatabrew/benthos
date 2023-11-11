package mock_test

import (
	"github.com/usedatabrew/benthos/v4/internal/component/output"
	"github.com/usedatabrew/benthos/v4/internal/manager/mock"
)

var _ output.Sync = mock.OutputWriter(nil)
