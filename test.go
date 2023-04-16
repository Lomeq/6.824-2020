package main

import(
	"sync"
)

func main(){
	var mu sync.Mutex
	a:=5
	go func(){
		mu.Lock()
		a=3
		mu.Unlock()
	}()
	mu.Lock()
	a=2
	mu.Unlock()
}