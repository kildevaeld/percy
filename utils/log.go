package utils

func DeferKeys(m map[string]interface{}) func() []string {

	return func() []string {
		s := make([]string, len(m))
		i := 0
		for k, _ := range m {
			s[i] = k
			i++
		}
		return s
	}
}
