package DHT

import (
	"math/big"
	"os"
)

func copyInfo(t InfoType) InfoType {
	return InfoType{t.IPAddr, new(big.Int).Set(t.NodeNum)}
}

func RedirectStderr(f *os.File) {
	//err := syscall.Dup2(int(f.Fd()), int(os.Stderr.Fd()))
	//if err != nil {
	//	log.Fatalf("Failed to redirect stderr to file: %v", err)
	//}
}
