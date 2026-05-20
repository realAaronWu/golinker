package rdma

// Conn is a ready-to-use RDMA connection. It embeds *PingPongConn for the
// data path (Send/Recv/SendRaw/RecvInPlace/CopyRecvToSend) and adds
// resource cleanup for the underlying CM objects.
//
// Created by Dial (client) or Listener.Accept (server).
type Conn struct {
	*PingPongConn
	cleanup func() // releases CM resources (event channel, CM ID, PD, CQs)
}

// Close tears down the data path (MRs, buffers) and releases CM resources.
func (c *Conn) Close() error {
	var err error
	if c.PingPongConn != nil {
		err = c.PingPongConn.Close()
	}
	if c.cleanup != nil {
		c.cleanup()
	}
	return err
}
