package main

import (
	"os"

	"knative.dev/security-guard/pkg/qpoption"
	"knative.dev/serving/pkg/queue/sharedmain"

	_ "github.com/ATNoG/knative-freezer-plugin/pkg/freezer"
)

func main() {
	qOpt := qpoption.NewGateQPOption()
	defer qOpt.Shutdown()

	if sharedmain.Main(qOpt.Setup) != nil {
		qOpt.Shutdown()
		os.Exit(1)
	}
}
