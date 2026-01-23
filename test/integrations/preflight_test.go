package integrations

import (
	"net/http"
	"testing"

	"github.com/OpenFogStack/tinyFaaS/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestPreflight(t *testing.T) {
	baseURL := testutil.RequireTinyFaaS(t)

	resp, err := http.Get(baseURL + "/system/list")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}
