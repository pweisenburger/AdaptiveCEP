package com.lambdarookie.eventscala.dsl

import com.lambdarookie.eventscala.backend.data.QoSUnits._
import com.lambdarookie.eventscala.backend.qos.QualityOfService._
import com.lambdarookie.eventscala.data.Events._
import com.lambdarookie.eventscala.data.Queries._

object Dsl {

  def slidingWindow  (instances: Instances): Window = SlidingInstances  (instances.getInstanceNum)
  def slidingWindow  (timeSpan: TimeSpan):   Window = SlidingTime       (timeSpan.toSeconds.toInt)
  def tumblingWindow (instances: Instances): Window = TumblingInstances (instances.getInstanceNum)
  def tumblingWindow (timeSpan: TimeSpan):   Window = TumblingTime      (timeSpan.toSeconds.toInt)

  def nStream[A]                (publisherName: String): NStream1[A] =                NStream1 (publisherName)
  def nStream[A, B]             (publisherName: String): NStream2[A, B] =             NStream2 (publisherName)
  def nStream[A, B, C]          (publisherName: String): NStream3[A, B, C] =          NStream3 (publisherName)
  def nStream[A, B, C, D]       (publisherName: String): NStream4[A, B, C, D] =       NStream4 (publisherName)
  def nStream[A, B, C, D, E]    (publisherName: String): NStream5[A, B, C, D, E] =    NStream5 (publisherName)

  def stream[A]                (publisherName: String, demands: Demand*): Query1[A] =                Stream1 (publisherName, demands.toSet)
  def stream[A, B]             (publisherName: String, demands: Demand*): Query2[A, B] =             Stream2 (publisherName, demands.toSet)
  def stream[A, B, C]          (publisherName: String, demands: Demand*): Query3[A, B, C] =          Stream3 (publisherName, demands.toSet)
  def stream[A, B, C, D]       (publisherName: String, demands: Demand*): Query4[A, B, C, D] =       Stream4 (publisherName, demands.toSet)
  def stream[A, B, C, D, E]    (publisherName: String, demands: Demand*): Query5[A, B, C, D, E] =    Stream5 (publisherName, demands.toSet)
  def stream[A, B, C, D, E, F] (publisherName: String, demands: Demand*): Query6[A, B, C, D, E, F] = Stream6 (publisherName, demands.toSet)

  case class Sequence1Helper[A](s: NStream1[A]) {
    def ->[B]             (s2: NStream1[B]            ): (NStream1[A], NStream1[B]) =             (s, s2)
    def ->[B, C]          (s2: NStream2[B, C]         ): (NStream1[A], NStream2[B, C]) =          (s, s2)
    def ->[B, C, D]       (s2: NStream3[B, C, D]      ): (NStream1[A], NStream3[B, C, D]) =       (s, s2)
    def ->[B, C, D, E]    (s2: NStream4[B, C, D, E]   ): (NStream1[A], NStream4[B, C, D, E]) =    (s, s2)
    def ->[B, C, D, E, F] (s2: NStream5[B, C, D, E, F]): (NStream1[A], NStream5[B, C, D, E, F]) = (s, s2)
  }

  case class Sequence2Helper[A, B](s: NStream2[A, B]) {
    def ->[C]          (s2: NStream1[C]         ): (NStream2[A, B], NStream1[C]) =          (s, s2)
    def ->[C, D]       (s2: NStream2[C, D]      ): (NStream2[A, B], NStream2[C, D]) =       (s, s2)
    def ->[C, D, E]    (s2: NStream3[C, D, E]   ): (NStream2[A, B], NStream3[C, D, E]) =    (s, s2)
    def ->[C, D, E, F] (s2: NStream4[C, D, E, F]): (NStream2[A, B], NStream4[C, D, E, F]) = (s, s2)
  }

  case class Sequence3Helper[A, B, C](s: NStream3[A, B, C]) {
    def ->[D]       (s2: NStream1[D]      ): (NStream3[A, B, C], NStream1[D]) =       (s, s2)
    def ->[D, E]    (s2: NStream2[D, E]   ): (NStream3[A, B, C], NStream2[D, E]) =    (s, s2)
    def ->[D, E, F] (s2: NStream3[D, E, F]): (NStream3[A, B, C], NStream3[D, E, F]) = (s, s2)
  }

  case class Sequence4Helper[A, B, C, D](s: NStream4[A, B, C, D]) {
    def ->[E]    (s2: NStream1[E]   ): (NStream4[A, B, C, D], NStream1[E]) =    (s, s2)
    def ->[E, F] (s2: NStream2[E, F]): (NStream4[A, B, C, D], NStream2[E, F]) = (s, s2)
  }

  case class Sequence5Helper[A, B, C, D, E](s: NStream5[A, B, C, D, E]) {
    def ->[F] (s2: NStream1[F]): (NStream5[A, B, C, D, E], NStream1[F]) = (s, s2)
  }

  implicit def nStream1ToSequence1Helper[A]             (s: NStream1[A]):             Sequence1Helper[A] =             Sequence1Helper(s)
  implicit def nStream2ToSequence2Helper[A, B]          (s: NStream2[A, B]):          Sequence2Helper[A, B] =          Sequence2Helper(s)
  implicit def nStream3ToSequence3Helper[A, B, C]       (s: NStream3[A, B, C]):       Sequence3Helper[A, B, C] =       Sequence3Helper(s)
  implicit def nStream4ToSequence4Helper[A, B, C, D]    (s: NStream4[A, B, C, D]):    Sequence4Helper[A, B, C, D] =    Sequence4Helper(s)
  implicit def nStream5ToSequence5Helper[A, B, C, D, E] (s: NStream5[A, B, C, D, E]): Sequence5Helper[A, B, C, D, E] = Sequence5Helper(s)

  def sequence[A, B]             (tuple: (NStream1[A],             NStream1[B]),             demands: Demand*): Sequence11[A, B] =             Sequence11 (tuple._1, tuple._2, demands.toSet)
  def sequence[A, B, C]          (tuple: (NStream1[A],             NStream2[B, C]),          demands: Demand*): Sequence12[A, B, C] =          Sequence12 (tuple._1, tuple._2, demands.toSet)
  def sequence[A, B, C]          (tuple: (NStream2[A, B],          NStream1[C]),             demands: Demand*): Sequence21[A, B, C] =          Sequence21 (tuple._1, tuple._2, demands.toSet)
  def sequence[A, B, C, D]       (tuple: (NStream1[A],             NStream3[B, C, D]),       demands: Demand*): Sequence13[A, B, C, D] =       Sequence13 (tuple._1, tuple._2, demands.toSet)
  def sequence[A, B, C, D]       (tuple: (NStream2[A, B],          NStream2[C, D]),          demands: Demand*): Sequence22[A, B, C, D] =       Sequence22 (tuple._1, tuple._2, demands.toSet)
  def sequence[A, B, C, D]       (tuple: (NStream3[A, B, C],       NStream1[D]),             demands: Demand*): Sequence31[A, B, C, D] =       Sequence31 (tuple._1, tuple._2, demands.toSet)
  def sequence[A, B, C, D, E]    (tuple: (NStream1[A],             NStream4[B, C, D, E]),    demands: Demand*): Sequence14[A, B, C, D, E] =    Sequence14 (tuple._1, tuple._2, demands.toSet)
  def sequence[A, B, C, D, E]    (tuple: (NStream2[A, B],          NStream3[C, D, E]),       demands: Demand*): Sequence23[A, B, C, D, E] =    Sequence23 (tuple._1, tuple._2, demands.toSet)
  def sequence[A, B, C, D, E]    (tuple: (NStream3[A, B, C],       NStream2[D, E]),          demands: Demand*): Sequence32[A, B, C, D, E] =    Sequence32 (tuple._1, tuple._2, demands.toSet)
  def sequence[A, B, C, D, E]    (tuple: (NStream4[A, B, C, D],    NStream1[E]),             demands: Demand*): Sequence41[A, B, C, D, E] =    Sequence41 (tuple._1, tuple._2, demands.toSet)
  def sequence[A, B, C, D, E, F] (tuple: (NStream1[A],             NStream5[B, C, D, E, F]), demands: Demand*): Sequence15[A, B, C, D, E, F] = Sequence15 (tuple._1, tuple._2, demands.toSet)
  def sequence[A, B, C, D, E, F] (tuple: (NStream2[A, B],          NStream4[C, D, E, F]),    demands: Demand*): Sequence24[A, B, C, D, E, F] = Sequence24 (tuple._1, tuple._2, demands.toSet)
  def sequence[A, B, C, D, E, F] (tuple: (NStream3[A, B, C],       NStream3[D, E, F]),       demands: Demand*): Sequence33[A, B, C, D, E, F] = Sequence33 (tuple._1, tuple._2, demands.toSet)
  def sequence[A, B, C, D, E, F] (tuple: (NStream4[A, B, C, D],    NStream2[E, F]),          demands: Demand*): Sequence42[A, B, C, D, E, F] = Sequence42 (tuple._1, tuple._2, demands.toSet)
  def sequence[A, B, C, D, E, F] (tuple: (NStream5[A, B, C, D, E], NStream1[F]),             demands: Demand*): Sequence51[A, B, C, D, E, F] = Sequence51 (tuple._1, tuple._2, demands.toSet)

  case class Query1Helper[A](q: Query1[A]) {
    def where                (                              cond: (A) => Boolean,   demands: Demand*): Query1[A] =                                                                                  Filter1       (q, toFunEventBoolean(cond), demands.toSet)
    def selfJoin             (                              w1: Window, w2: Window, demands: Demand*): Query2[A, A] =                                                                               SelfJoin11    (q, w1, w2,                  demands.toSet)
    def join[B]              (q2: Query1[B],                w1: Window, w2: Window, demands: Demand*): Query2[A, B] =                                                                               Join11        (q, q2, w1, w2,              demands.toSet)
    def join[B, C]           (q2: Query2[B, C],             w1: Window, w2: Window, demands: Demand*): Query3[A, B, C] =                                                                            Join12        (q, q2, w1, w2,              demands.toSet)
    def join[B, C, D]        (q2: Query3[B, C, D],          w1: Window, w2: Window, demands: Demand*): Query4[A, B, C, D] =                                                                         Join13        (q, q2, w1, w2,              demands.toSet)
    def join[B, C, D, E]     (q2: Query4[B, C, D, E],       w1: Window, w2: Window, demands: Demand*): Query5[A, B, C, D, E] =                                                                      Join14        (q, q2, w1, w2,              demands.toSet)
    def join[B, C, D, E, F]  (q2: Query5[B, C, D, E, F],    w1: Window, w2: Window, demands: Demand*): Query6[A, B, C, D, E, F] =                                                                   Join15        (q, q2, w1, w2,              demands.toSet)
    def and[B]               (q2: Query1[B],                                        demands: Demand*): Query2[A, B] =                                                                               Conjunction11 (q, q2,                      demands.toSet)
    def and[B, C]            (q2: Query2[B, C],                                     demands: Demand*): Query3[A, B, C] =                                                                            Conjunction12 (q, q2,                      demands.toSet)
    def and[B, C, D]         (q2: Query3[B, C, D],                                  demands: Demand*): Query4[A, B, C, D] =                                                                         Conjunction13 (q, q2,                      demands.toSet)
    def and[B, C, D, E]      (q2: Query4[B, C, D, E],                               demands: Demand*): Query5[A, B, C, D, E] =                                                                      Conjunction14 (q, q2,                      demands.toSet)
    def and[B, C, D, E, F]   (q2: Query5[B, C, D, E, F],                            demands: Demand*): Query6[A, B, C, D, E, F] =                                                                   Conjunction15 (q, q2,                      demands.toSet)
    def or[B]                (q2: Query1[B],                                        demands: Demand*): Query1[Either[A, B]] =                                                                       Disjunction11 (q, q2,                      demands.toSet)
    def or[B, C]             (q2: Query2[B, C],                                     demands: Demand*): Query2[Either[A, B], Either[X, C]] =                                                         Disjunction12 (q, q2,                      demands.toSet)
    def or[B, C, D]          (q2: Query3[B, C, D],                                  demands: Demand*): Query3[Either[A, B], Either[X, C], Either[X, D]] =                                           Disjunction13 (q, q2,                      demands.toSet)
    def or[B, C, D, E]       (q2: Query4[B, C, D, E],                               demands: Demand*): Query4[Either[A, B], Either[X, C], Either[X, D], Either[X, E]] =                             Disjunction14 (q, q2,                      demands.toSet)
    def or[B, C, D, E, F]    (q2: Query5[B, C, D, E, F],                            demands: Demand*): Query5[Either[A, B], Either[X, C], Either[X, D], Either[X, E], Either[X, F]] =               Disjunction15 (q, q2,                      demands.toSet)
    def or[B, C, D, E, F, G] (q2: Query6[B, C, D, E, F, G],                         demands: Demand*): Query6[Either[A, B], Either[X, C], Either[X, D], Either[X, E], Either[X, F], Either[X, G]] = Disjunction16 (q, q2,                      demands.toSet)
  }

  case class Query2Helper[A, B](q: Query2[A, B]) {
    def where                (                              cond: (A, B) => Boolean, demands: Demand*): Query2[A, B] =                                                                               Filter2       (q, toFunEventBoolean(cond), demands.toSet)
    def dropElem1            (                                                       demands: Demand*): Query1[B] =                                                                                  DropElem1Of2  (q,                          demands.toSet)
    def dropElem2            (                                                       demands: Demand*): Query1[A] =                                                                                  DropElem2Of2  (q,                          demands.toSet)
    def selfJoin             (                              w1: Window, w2: Window,  demands: Demand*): Query4[A, B, A, B] =                                                                         SelfJoin22    (q, w1, w2,                  demands.toSet)
    def join[C]              (q2: Query1[C],                w1: Window, w2: Window,  demands: Demand*): Query3[A, B, C] =                                                                            Join21        (q, q2, w1, w2,              demands.toSet)
    def join[C, D]           (q2: Query2[C, D],             w1: Window, w2: Window,  demands: Demand*): Query4[A, B, C, D] =                                                                         Join22        (q, q2, w1, w2,              demands.toSet)
    def join[C, D, E]        (q2: Query3[C, D, E],          w1: Window, w2: Window,  demands: Demand*): Query5[A, B, C, D, E] =                                                                      Join23        (q, q2, w1, w2,              demands.toSet)
    def join[C, D, E, F]     (q2: Query4[C, D, E, F],       w1: Window, w2: Window,  demands: Demand*): Query6[A, B, C, D, E, F] =                                                                   Join24        (q, q2, w1, w2,              demands.toSet)
    def and[C]               (q2: Query1[C],                                         demands: Demand*): Query3[A, B, C] =                                                                            Conjunction21 (q, q2,                      demands.toSet)
    def and[C, D]            (q2: Query2[C, D],                                      demands: Demand*): Query4[A, B, C, D] =                                                                         Conjunction22 (q, q2,                      demands.toSet)
    def and[C, D, E]         (q2: Query3[C, D, E],                                   demands: Demand*): Query5[A, B, C, D, E] =                                                                      Conjunction23 (q, q2,                      demands.toSet)
    def and[C, D, E, F]      (q2: Query4[C, D, E, F],                                demands: Demand*): Query6[A, B, C, D, E, F] =                                                                   Conjunction24 (q, q2,                      demands.toSet)
    def or[C]                (q2: Query1[C],                                         demands: Demand*): Query2[Either[A, C], Either[B, X]] =                                                         Disjunction21 (q, q2,                      demands.toSet)
    def or[C, D]             (q2: Query2[C, D],                                      demands: Demand*): Query2[Either[A, C], Either[B, D]] =                                                         Disjunction22 (q, q2,                      demands.toSet)
    def or[C, D, E]          (q2: Query3[C, D, E],                                   demands: Demand*): Query3[Either[A, C], Either[B, D], Either[X, E]] =                                           Disjunction23 (q, q2,                      demands.toSet)
    def or[C, D, E, F]       (q2: Query4[C, D, E, F],                                demands: Demand*): Query4[Either[A, C], Either[B, D], Either[X, E], Either[X, F]] =                             Disjunction24 (q, q2,                      demands.toSet)
    def or[C, D, E, F, G]    (q2: Query5[C, D, E, F, G],                             demands: Demand*): Query5[Either[A, C], Either[B, D], Either[X, E], Either[X, F], Either[X, G]] =               Disjunction25 (q, q2,                      demands.toSet)
    def or[C, D, E, F, G, H] (q2: Query6[C, D, E, F, G, H],                          demands: Demand*): Query6[Either[A, C], Either[B, D], Either[X, E], Either[X, F], Either[X, G], Either[X, H]] = Disjunction26 (q, q2,                      demands.toSet)
  }

  case class Query3Helper[A, B, C](q: Query3[A, B, C]) {
    def where                (                              cond: (A, B, C) => Boolean, demands: Demand*): Query3[A, B, C] =                                                                            Filter3       (q, toFunEventBoolean(cond), demands.toSet)
    def dropElem1            (                                                          demands: Demand*): Query2[B, C] =                                                                               DropElem1Of3  (q,                          demands.toSet)
    def dropElem2            (                                                          demands: Demand*): Query2[A, C] =                                                                               DropElem2Of3  (q,                          demands.toSet)
    def dropElem3            (                                                          demands: Demand*): Query2[A, B] =                                                                               DropElem3Of3  (q,                          demands.toSet)
    def selfJoin             (                              w1: Window, w2: Window,     demands: Demand*): Query6[A, B, C, A, B, C] =                                                                   SelfJoin33    (q, w1, w2,                  demands.toSet)
    def join[D]              (q2: Query1[D],                w1: Window, w2: Window,     demands: Demand*): Query4[A, B, C, D] =                                                                         Join31        (q, q2, w1, w2,              demands.toSet)
    def join[D, E]           (q2: Query2[D, E],             w1: Window, w2: Window,     demands: Demand*): Query5[A, B, C, D, E] =                                                                      Join32        (q, q2, w1, w2,              demands.toSet)
    def join[D, E, F]        (q2: Query3[D, E, F],          w1: Window, w2: Window,     demands: Demand*): Query6[A, B, C, D, E, F] =                                                                   Join33        (q, q2, w1, w2,              demands.toSet)
    def and[D]               (q2: Query1[D],                                            demands: Demand*): Query4[A, B, C, D] =                                                                         Conjunction31 (q, q2,                      demands.toSet)
    def and[D, E]            (q2: Query2[D, E],                                         demands: Demand*): Query5[A, B, C, D, E] =                                                                      Conjunction32 (q, q2,                      demands.toSet)
    def and[D, E, F]         (q2: Query3[D, E, F],                                      demands: Demand*): Query6[A, B, C, D, E, F] =                                                                   Conjunction33 (q, q2,                      demands.toSet)
    def or[D]                (q2: Query1[D],                                            demands: Demand*): Query3[Either[A, D], Either[B, X], Either[C, X]] =                                           Disjunction31 (q, q2,                      demands.toSet)
    def or[D, E]             (q2: Query2[D, E],                                         demands: Demand*): Query3[Either[A, D], Either[B, E], Either[C, X]] =                                           Disjunction32 (q, q2,                      demands.toSet)
    def or[D, E, F]          (q2: Query3[D, E, F],                                      demands: Demand*): Query3[Either[A, D], Either[B, E], Either[C, F]] =                                           Disjunction33 (q, q2,                      demands.toSet)
    def or[D, E, F, G]       (q2: Query4[D, E, F, G],                                   demands: Demand*): Query4[Either[A, D], Either[B, E], Either[C, F], Either[X, G]] =                             Disjunction34 (q, q2,                      demands.toSet)
    def or[D, E, F, G, H]    (q2: Query5[D, E, F, G, H],                                demands: Demand*): Query5[Either[A, D], Either[B, E], Either[C, F], Either[X, G], Either[X, H]] =               Disjunction35 (q, q2,                      demands.toSet)
    def or[D, E, F, G, H, I] (q2: Query6[D, E, F, G, H, I],                             demands: Demand*): Query6[Either[A, D], Either[B, E], Either[C, F], Either[X, G], Either[X, H], Either[X, I]] = Disjunction36 (q, q2,                      demands.toSet)
  }

  case class Query4Helper[A, B, C, D](q: Query4[A, B, C, D]) {
    def where                (                              cond: (A, B, C, D) => Boolean, demands: Demand*): Query4[A, B, C, D] =                                                                         Filter4       (q, toFunEventBoolean(cond), demands.toSet)
    def dropElem1            (                                                             demands: Demand*): Query3[B, C, D] =                                                                            DropElem1Of4  (q,                          demands.toSet)
    def dropElem2            (                                                             demands: Demand*): Query3[A, C, D] =                                                                            DropElem2Of4  (q,                          demands.toSet)
    def dropElem3            (                                                             demands: Demand*): Query3[A, B, D] =                                                                            DropElem3Of4  (q,                          demands.toSet)
    def dropElem4            (                                                             demands: Demand*): Query3[A, B, C] =                                                                            DropElem4Of4  (q,                          demands.toSet)
    def join[E]              (q2: Query1[E],                w1: Window, w2: Window,        demands: Demand*): Query5[A, B, C, D, E] =                                                                      Join41        (q, q2, w1, w2,              demands.toSet)
    def join[E, F]           (q2: Query2[E, F],             w1: Window, w2: Window,        demands: Demand*): Query6[A, B, C, D, E, F] =                                                                   Join42        (q, q2, w1, w2,              demands.toSet)
    def and[E]               (q2: Query1[E],                                               demands: Demand*): Query5[A, B, C, D, E] =                                                                      Conjunction41 (q, q2,                      demands.toSet)
    def and[E, F]            (q2: Query2[E, F],                                            demands: Demand*): Query6[A, B, C, D, E, F] =                                                                   Conjunction42 (q, q2,                      demands.toSet)
    def or[E]                (q2: Query1[E],                                               demands: Demand*): Query4[Either[A, E], Either[B, X], Either[C, X], Either[D, X]] =                             Disjunction41 (q, q2,                      demands.toSet)
    def or[E, F]             (q2: Query2[E, F],                                            demands: Demand*): Query4[Either[A, E], Either[B, F], Either[C, X], Either[D, X]] =                             Disjunction42 (q, q2,                      demands.toSet)
    def or[E, F, G]          (q2: Query3[E, F, G],                                         demands: Demand*): Query4[Either[A, E], Either[B, F], Either[C, G], Either[D, X]] =                             Disjunction43 (q, q2,                      demands.toSet)
    def or[E, F, G, H]       (q2: Query4[E, F, G, H],                                      demands: Demand*): Query4[Either[A, E], Either[B, F], Either[C, G], Either[D, H]] =                             Disjunction44 (q, q2,                      demands.toSet)
    def or[E, F, G, H, I]    (q2: Query5[E, F, G, H, I],                                   demands: Demand*): Query5[Either[A, E], Either[B, F], Either[C, G], Either[D, H], Either[X, I]] =               Disjunction45 (q, q2,                      demands.toSet)
    def or[E, F, G, H, I, J] (q2: Query6[E, F, G, H, I, J],                                demands: Demand*): Query6[Either[A, E], Either[B, F], Either[C, G], Either[D, H], Either[X, I], Either[X, J]] = Disjunction46 (q, q2,                      demands.toSet)
  }

  case class Query5Helper[A, B, C, D, E](q: Query5[A, B, C, D, E]) {
    def where                (                              cond: (A, B, C, D, E) => Boolean, demands: Demand*): Query5[A, B, C, D, E] =                                                                      Filter5       (q, toFunEventBoolean(cond), demands.toSet)
    def dropElem1       (                                                                demands: Demand*): Query4[B, C, D, E] =                                                                              DropElem1Of5  (q,                          demands.toSet)
    def dropElem2       (                                                                demands: Demand*): Query4[A, C, D, E] =                                                                              DropElem2Of5  (q,                          demands.toSet)
    def dropElem3       (                                                                demands: Demand*): Query4[A, B, D, E] =                                                                              DropElem3Of5  (q,                          demands.toSet)
    def dropElem4       (                                                                demands: Demand*): Query4[A, B, C, E] =                                                                              DropElem4Of5  (q,                          demands.toSet)
    def dropElem5       (                                                                demands: Demand*): Query4[A, B, C, D] =                                                                              DropElem5Of5  (q,                          demands.toSet)
    def join[F]              (q2: Query1[F],                w1: Window, w2: Window,           demands: Demand*): Query6[A, B, C, D, E, F] =                                                                   Join51        (q, q2, w1, w2,              demands.toSet)
    def and[F]               (q2: Query1[F],                                                  demands: Demand*): Query6[A, B, C, D, E, F] =                                                                   Conjunction51 (q, q2,                      demands.toSet)
    def or[F]                (q2: Query1[F],                                                  demands: Demand*): Query5[Either[A, F], Either[B, X], Either[C, X], Either[D, X], Either[E, X]] =               Disjunction51 (q, q2,                      demands.toSet)
    def or[F, G]             (q2: Query2[F, G],                                               demands: Demand*): Query5[Either[A, F], Either[B, G], Either[C, X], Either[D, X], Either[E, X]] =               Disjunction52 (q, q2,                      demands.toSet)
    def or[F, G, H]          (q2: Query3[F, G, H],                                            demands: Demand*): Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, X], Either[E, X]] =               Disjunction53 (q, q2,                      demands.toSet)
    def or[F, G, H, I]       (q2: Query4[F, G, H, I],                                         demands: Demand*): Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, X]] =               Disjunction54 (q, q2,                      demands.toSet)
    def or[F, G, H, I, J]    (q2: Query5[F, G, H, I, J],                                      demands: Demand*): Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J]] =               Disjunction55 (q, q2,                      demands.toSet)
    def or[F, G, H, I, J, K] (q2: Query6[F, G, H, I, J, K],                                   demands: Demand*): Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[X, K]] = Disjunction56 (q, q2,                      demands.toSet)
  }

  case class Query6Helper[A, B, C, D, E, F](q: Query6[A, B, C, D, E, F]) {
    def where                (                              cond: (A, B, C, D, E, F) => Boolean, demands: Demand*): Query6[A, B, C, D, E, F] =                                                                   Filter6           (q, toFunEventBoolean(cond), demands.toSet)
    def dropElem1            (                                                                   demands: Demand*): Query5[B, C, D, E, F] =                                                                      DropElem1Of6      (q,                          demands.toSet)
    def dropElem2            (                                                                   demands: Demand*): Query5[A, C, D, E, F] =                                                                      DropElem2Of6      (q,                          demands.toSet)
    def dropElem3            (                                                                   demands: Demand*): Query5[A, B, D, E, F] =                                                                      DropElem3Of6      (q,                          demands.toSet)
    def dropElem4            (                                                                   demands: Demand*): Query5[A, B, C, E, F] =                                                                      DropElem4Of6      (q,                          demands.toSet)
    def dropElem5            (                                                                   demands: Demand*): Query5[A, B, C, D, F] =                                                                      DropElem5Of6      (q,                          demands.toSet)
    def dropElem6            (                                                                   demands: Demand*): Query5[A, B, C, D, E] =                                                                      DropElem6Of6      (q,                          demands.toSet)
    def or[G]                (q2: Query1[G],                                                     demands: Demand*): Query6[Either[A, F], Either[B, X], Either[C, X], Either[D, X], Either[E, X], Either[F, X]] = Disjunction61     (q, q2,                      demands.toSet)
    def or[G, H]             (q2: Query2[G, H],                                                  demands: Demand*): Query6[Either[A, F], Either[B, G], Either[C, X], Either[D, X], Either[E, X], Either[F, X]] = Disjunction62     (q, q2,                      demands.toSet)
    def or[G, H, I]          (q2: Query3[G, H, I],                                               demands: Demand*): Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, X], Either[E, X], Either[F, X]] = Disjunction63     (q, q2,                      demands.toSet)
    def or[G, H, I, J]       (q2: Query4[G, H, I, J],                                            demands: Demand*): Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, X], Either[F, X]] = Disjunction64     (q, q2,                      demands.toSet)
    def or[G, H, I, J, K]    (q2: Query5[G, H, I, J, K],                                         demands: Demand*): Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[F, X]] = Disjunction65     (q, q2,                      demands.toSet)
    def or[G, H, I, J, K, L] (q2: Query6[G, H, I, J, K, L],                                      demands: Demand*): Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[F, K]] = Disjunction66     (q, q2,                      demands.toSet)
  }

  implicit def query1ToQuery1Helper[A]                (q: Query1[A]):                Query1Helper[A] =                Query1Helper (q)
  implicit def query2ToQuery2Helper[A, B]             (q: Query2[A, B]):             Query2Helper[A, B] =             Query2Helper (q)
  implicit def query3ToQuery3Helper[A, B, C]          (q: Query3[A, B, C]):          Query3Helper[A, B, C] =          Query3Helper (q)
  implicit def query4ToQuery4Helper[A, B, C, D]       (q: Query4[A, B, C, D]):       Query4Helper[A, B, C, D] =       Query4Helper (q)
  implicit def query5ToQuery5Helper[A, B, C, D, E]    (q: Query5[A, B, C, D, E]):    Query5Helper[A, B, C, D, E] =    Query5Helper (q)
  implicit def query6ToQuery6Helper[A, B, C, D, E, F] (q: Query6[A, B, C ,D, E, F]): Query6Helper[A, B, C, D, E, F] = Query6Helper (q)

}
