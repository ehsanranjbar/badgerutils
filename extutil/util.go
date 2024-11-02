package extutil

func findAs[T any](anys []any) (t T, ok bool) {
	for _, a := range anys {
		if t, ok := a.(T); ok {
			return t, true
		}
	}

	return t, false
}
