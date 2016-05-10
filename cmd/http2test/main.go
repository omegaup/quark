package main

import (
	"fmt"
	"net/http"
)

func main() {
	client := &http.Client{
		Transport: &http.Transport{
			ExpectContinueTimeout: 0,
		},
	}
	r, err := client.Get("https://http2.akamai.com/demo")
	if err != nil {
		panic(err)
	}
	fmt.Printf("HTTP %d\n", r.ProtoMajor)
	r.Body.Close()
}
