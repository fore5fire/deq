package backup

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const Extension = ".deqbackup"
const IndexName = "index" + Extension

func NameForVersion(v uint64) string {
	return fmt.Sprintf("%016x%s", v, Extension)
}

func VersionForName(name string) (uint64, error) {
	trimmed := strings.TrimSuffix(name, Extension)
	if trimmed == name {
		return 0, errors.New("incorrect extension")
	}
	return strconv.ParseUint(trimmed, 16, 64)
}
