package share

import (
	"log"
	"net"
)

var localIpV4 string

func init() {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Fatalln("check net interfaces error:" + err.Error())
	}
	for _, address := range addrs {
		// 检查 ip 地址判断是不是回环地址（127.0.0.1）
		if ipNet, ok := address.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				localIpV4 = ipNet.IP.String()
				break
			}
		}
	}
}

func LocalIpv4() string {
	return localIpV4
}
