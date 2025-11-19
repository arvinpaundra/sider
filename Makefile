bench:
	go test -v -bench=BenchmarkServer_SetCommand -benchmem -benchtime=10s -cpuprofile=cpu.out -run=""