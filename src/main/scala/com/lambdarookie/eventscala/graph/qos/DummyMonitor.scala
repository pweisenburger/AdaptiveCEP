package com.lambdarookie.eventscala.graph.qos

case class DummyMonitorFactory() extends MonitorFactory {

  override def createLeafNodeMonitor: LeafNodeMonitor = new LeafNodeMonitor {}
  override def createUnaryNodeMonitor: UnaryNodeMonitor = new UnaryNodeMonitor {}
  override def createBinaryNodeMonitor: BinaryNodeMonitor = new BinaryNodeMonitor {}

}
