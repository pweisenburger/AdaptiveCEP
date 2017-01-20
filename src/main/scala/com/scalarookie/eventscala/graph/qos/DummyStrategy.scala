package com.scalarookie.eventscala.graph.qos

case class DummyStrategyFactory() extends StrategyFactory {

  override def getLeafNodeStrategy: LeafNodeStrategy = new LeafNodeStrategy {}
  override def getUnaryNodeStrategy: UnaryNodeStrategy = new UnaryNodeStrategy {}
  override def getBinaryNodeStrategy: BinaryNodeStrategy = new BinaryNodeStrategy {}

}
