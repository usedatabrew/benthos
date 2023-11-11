package mock_test

import (
	"github.com/usedatabrew/benthos/v4/internal/bundle"
	"github.com/usedatabrew/benthos/v4/internal/manager/mock"
)

var _ bundle.NewManagement = &mock.Manager{}
