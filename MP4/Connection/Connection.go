package conn

import (
	"fmt"
	"net"
	"log"
	"os"

	MP "../MsgProtocol"
)

func BuildUDPServer(port string) *net.UDPConn {
        udpAddr, err := net.ResolveUDPAddr("udp", ":"+ port)
        if err != nil {
                log.Fatal(err)
        }

        ln, err := net.ListenUDP("udp", udpAddr)
        if err != nil {
                log.Fatal(err)
        }
        fmt.Println("===BuildUDPServer: Listen on Port" + port)

        return ln
}

func BuildUDPClient(svAddr string, port string) *net.UDPConn{
	var addrPort string
	if svAddr == "" {
		addrPort = ":" + port
	} else {
		addrPort = svAddr + ":" + port
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addrPort)
	if err != nil {
		fmt.Println("BuildUDPClient: ResolveErr")
		log.Println(err.Error())
	}

	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		fmt.Println("BuildUDPCLient: DialErr")
		log.Println(err.Error())
	}

	return conn
}

func WriteToUDPConn(pkg []byte, conn *net.UDPConn) {
	_, err := conn.Write(pkg)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
}

func ReadUDP(conn *net.UDPConn) (int, []byte){
	buf := make([]byte, 2048)
	n, err := conn.Read(buf)
	if err != nil {
		log.Println(err.Error())
		return -1, []byte{}
	}
	return n,buf
}

func CloseLocalPort(nodeID string, port string) {
	leaveMsg := MP.NewMessage(MP.LeaveMsg, nodeID, []string{nodeID})
	leavePkg := MP.MsgToJSON(leaveMsg)

	conn := BuildUDPClient("", port)
	WriteToUDPConn(leavePkg, conn)

	conn.Close()
	fmt.Printf("Port %s Closed!!", port) 
}
