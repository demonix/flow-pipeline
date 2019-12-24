package main

import (
	"log"
	"net"
)

func Hosts(cidr string) ([]string, error) {
	ip, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, err
	}

	var ips []string
	for ip := ip.Mask(ipnet.Mask); ipnet.Contains(ip); inc(ip) {
		ips = append(ips, ip.String())
	}

	// remove network address and broadcast address
	lenIPs := len(ips)
	switch {
	case lenIPs < 2:
		return ips, nil

	default:
		return ips[1 : len(ips)-1], nil
	}
}

func inc(ip net.IP) {
	for j := len(ip) - 1; j >= 0; j-- {
		ip[j]++
		if ip[j] > 0 {
			break
		}
	}
}

func getNetWorks(NetworksDict map[string]string) {
	NetworksDict = make(map[string]string)
	tmp, err := Hosts("192.168.1.1/24")
	if err != nil {
		log.Fatal(err)
	}

	for _, curFlow := range tmp {
		NetworksDict[curFlow] = "FirstProject"
	}

	tmp, err := Hosts("192.168.1.2/24")
	if err != nil {
		log.Fatal(err)
	}
	for _, curFlow := range tmp {
		NetworksDict[curFlow] = "SecondProject"
	}

}
