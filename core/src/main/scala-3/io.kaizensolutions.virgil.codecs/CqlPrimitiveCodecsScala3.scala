import scala.{*:, NonEmptyTuple}
import com.datastax.oss.driver.api.core.`type`.*
import com.datastax.oss.driver.api.core.data.TupleValue
import io.kaizensolutions.virgil.*
import io.kaizensolutions.virgil.codecs.CqlPrimitiveEncoder
import scala.deriving.Mirror
import io.kaizensolutions.virgil.codecs.CqlPrimitiveDecoder
import scala.compiletime.ops.int.*

trait ToCassandraTuple[T] {
  def from(todo: T): TupleValue
}

trait FromCassandraTuple[T <: NonEmptyTuple] {
  def to(tuple: TupleValue): T
}

trait ScalaToCassandraType[T] {
  def get: DataType
}

trait TupleValueGetter[T, I <: Int] {
  def get(value: TupleValue): T
}

object ScalaToCassandraType {
  given scalaIntToCassandra: ScalaToCassandraType[Int] with
    inline def get: DataType = DataTypes.INT

  given scalaStringToCassandra: ScalaToCassandraType[String] with
    inline def get: DataType = DataTypes.TEXT

  given scalaFloatToCassandra: ScalaToCassandraType[Float] with
    inline def get: DataType = DataTypes.FLOAT
}

object TupleValueGetter {
  given intValueGetterForTupleValue[I <: Int](using
    indexValue: ValueOf[I],
    decoder: CqlPrimitiveDecoder[Int]
  ): TupleValueGetter[Int, I] = new {
    def get(value: TupleValue): Int =
      CqlPrimitiveDecoder.decodePrimitiveByIndex(value, indexValue.value)
  }
}

object CqlPrimitiveEncoderScala3 {
  import CqlPrimitiveEncoder.*

  given scalaTuplePrimitiveEncoder[T <: Tuple](using
    makeTupleValue: ToCassandraTuple[T]
  ): CqlPrimitiveEncoder.WithDriver[T, TupleValue] =
    new CqlPrimitiveEncoder[T] {
      type DriverType = TupleValue
      def driverClass: Class[DriverType]                              = classOf[DriverType]
      def scala2Driver(scalaValue: T, dataType: DataType): DriverType = makeTupleValue.from(scalaValue)
    }

  type TupleValueSetter = TupleValue => TupleValue
  case class Acc[X <: Tuple](
    todo: X,
    index: Int,
    tupleTypesReversed: List[DataType],
    valueSetters: List[TupleValueSetter]
  ) {
    def next[NX <: Tuple](newTodo: NX, dataType: DataType, setter: TupleValueSetter): Acc[NX] =
      copy(newTodo, index + 1, dataType :: tupleTypesReversed, setter :: valueSetters)
  }

  inline given nonEmptyTupleToCassandra[X <: NonEmptyTuple](using
    encoder: ToCassandraTuple[Acc[X]]
  ): ToCassandraTuple[X] with
    def from(todo: X): TupleValue = encoder.from(Acc[X](todo, 0, List.empty, List.empty))

  inline given traverseTupleKeepingTypes[H: CqlPrimitiveEncoder: ScalaToCassandraType, T <: Tuple](using
    tailEncoder: ToCassandraTuple[Acc[T]]
  ): ToCassandraTuple[Acc[H *: T]] with
    def from(state: Acc[H *: T]): TupleValue =
      val current = state.todo.head
      val todo    = state.todo.tail
      val cType   = summon[ScalaToCassandraType[H]].get
      val setter  = (tv: TupleValue) => encodePrimitiveByIndex[TupleValue, H](tv, state.index, current)
      tailEncoder.from(state.next(todo, cType, setter))

  inline given accumulatedTypesToTupleValue[X <: EmptyTuple]: ToCassandraTuple[Acc[X]] with
    def from(finalState: Acc[X]): TupleValue =
      val allTypes   = finalState.tupleTypesReversed.reverse
      val tupleMaker = DataTypes.tupleOf(allTypes*)
      finalState.valueSetters.foldLeft(tupleMaker.newValue()) { case (tuple, setter) => setter(tuple) }
}

object CqlPrimitiveDecoderScala3 {
  case class Acc[X <: NonEmptyTuple](
    acc: X,
    index: Int,
    tupleTypesReversed: List[DataType]
  )

  inline given tupleFromCassandraTupleValueStart[T <: NonEmptyTuple](using m: Mirror.ProductOf[T], getter: TupleValueGetter[T, 0]): T = m fromProductTyped getter.get(???)

  inline given tupleFromCassandraTupleValue[H, T <: Tuple, R <: H *: T, I <: Int](using
    head: TupleValueGetter[H, I],
    tail: TupleValueGetter[T, I + 1]
  ): TupleValueGetter[H *: T, I] =
    new {
      def get(value: TupleValue): H *: T = head.get(value) *: tail.get(value)
    }
}
