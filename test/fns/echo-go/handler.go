package main

func Handle(body []byte, headers map[string]string) (string, error) {
	return string(body), nil
}
