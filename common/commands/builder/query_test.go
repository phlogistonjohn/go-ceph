package builder

import (
	"testing"

	"github.com/stretchr/testify/require"
	tsuite "github.com/stretchr/testify/suite"

	_ "github.com/ceph/go-ceph/common/admin/manager"
	"github.com/ceph/go-ceph/internal/admintest"
	_ "github.com/ceph/go-ceph/internal/commands"
)

func TestBuilder(t *testing.T) {
	tsuite.Run(t, new(CommandBuilderSuite))
}

// CommandBuilderSuite is a suite of tests for commands builder
type CommandBuilderSuite struct {
	tsuite.Suite

	vconn *admintest.Connector
}

func (suite *CommandBuilderSuite) SetupSuite() {
	suite.vconn = admintest.NewConnector()
}

func (suite *CommandBuilderSuite) TearDownSuite() {
}

func (suite *CommandBuilderSuite) TestQueryMonJSON() {
	t := suite.T()
	conn := suite.vconn.Get(t)
	j, err := QueryMonJSON(conn)
	require.NoError(t, err)
	require.NotEmpty(t, j)
}

/*
	t.Log(string(j))
	require.Empty(t, j)
*/

func (suite *CommandBuilderSuite) TestQueryMgrJSON() {
	t := suite.T()
	conn := suite.vconn.Get(t)
	j, err := QueryMgrJSON(conn)
	require.NoError(t, err)
	require.NotEmpty(t, j)
}

func (suite *CommandBuilderSuite) TestQueryMonDescriptions() {
	t := suite.T()
	conn := suite.vconn.Get(t)
	cd, err := QueryMonDescriptions(conn)
	require.NoError(t, err)
	require.NotEmpty(t, cd.Entries)
}

func (suite *CommandBuilderSuite) TestQueryMgrDescriptions() {
	t := suite.T()
	conn := suite.vconn.Get(t)
	cd, err := QueryMgrDescriptions(conn)
	require.NoError(t, err)
	require.NotEmpty(t, cd.Entries)
}
