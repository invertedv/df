package df

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMemLoad(t *testing.T) {
	cols, err := MemLoad("")
	assert.Nil(t, err)
	_, e := NewDF(cols...)
	assert.Nil(t, e)
}
