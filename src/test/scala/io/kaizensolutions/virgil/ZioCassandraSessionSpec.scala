package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.uuid.Uuids
import io.kaizensolutions.virgil.codecs.{Reader, Writer}
import io.kaizensolutions.virgil.configuration.{ConsistencyLevel, ExecutionAttributes}
import io.kaizensolutions.virgil.cql._
import zio._
import zio.duration._
import zio.random.Random
import zio.stream.ZStream
import zio.test.Assertion.hasSameElements
import zio.test.TestAspect._
import zio.test._
import zio.test.environment.Live

import java.nio.ByteBuffer
import java.util.UUID
import scala.util.Try

object ZioCassandraSessionSpec {
  def sessionSpec: Spec[Live with Has[ZioCassandraSession] with Random with Sized with TestConfig, TestFailure[
    Throwable
  ], TestSuccess] =
    suite("Cassandra Session Interpreter Specification") {
      (queries + actions) @@ timeout(1.minute) @@ samples(10)
    }

  def queries
    : Spec[Has[ZioCassandraSession] with Random with Sized with TestConfig, TestFailure[Throwable], TestSuccess] =
    suite("Queries") {
      testM("selectFirst") {
        ZioCassandraSession
          .selectFirst(
            input = cql"SELECT now() FROM system.local".query[SystemLocalResponse],
            config = ExecutionAttributes.default.withConsistencyLevel(ConsistencyLevel.LocalOne)
          )
          .map(result => assertTrue(result.flatMap(_.time.toOption).get > 0))
      } +
        testM("select") {
          ZioCassandraSession
            .select(
              input = cql"SELECT prepared_id, logged_keyspace, query_string FROM system.prepared_statements"
                .query[PreparedStatementsResponse],
              config = ExecutionAttributes.default.withConsistencyLevel(ConsistencyLevel.LocalOne)
            )
            .runCollect
            .map(results =>
              assertTrue(results.forall { r =>
                import r.{query_string => query}
                query.contains("SELECT") ||
                query.contains("UPDATE") ||
                query.contains("CREATE") ||
                query.contains("DELETE") ||
                query.contains("INSERT") ||
                query.contains("USE")
              })
            )
        } +
        testM("selectPage") {
          import SelectPageRow._
          checkM(Gen.chunkOfN(50)(gen)) { actual =>
            for {
              _  <- ZioCassandraSession.executeAction(truncate)
              _  <- ZIO.foreachPar_(actual.map(insert))(ZioCassandraSession.executeAction(_))
              all = ZioCassandraSession.select(selectAll).runCollect
              paged =
                selectPageStream(selectAll, ExecutionAttributes.default.withPageSize(actual.length / 2)).runCollect
              result                        <- all.zipPar(paged)
              (dataFromSelect, dataFromPage) = result
            } yield assert(dataFromPage)(hasSameElements(dataFromSelect)) &&
              assert(dataFromSelect)(hasSameElements(actual))
          }
        }
    }

  def actions
    : Spec[Has[ZioCassandraSession] with Random with Sized with TestConfig, TestFailure[Throwable], TestSuccess] =
    suite("Actions") {
      testM("executeAction") {
        import ExecuteTestTable._
        checkM(Gen.listOfN(10)(gen)) { actual =>
          val truncateData = ZioCassandraSession.executeAction(truncate(table))
          val toInsert     = actual.map(insert(table))
          val expected = ZioCassandraSession
            .select(selectAllIn(table)(actual.map(_.id)))
            .runCollect

          for {
            _        <- truncateData
            _        <- ZIO.foreachPar_(toInsert)(ZioCassandraSession.executeAction(_))
            expected <- expected
          } yield assert(actual)(hasSameElements(expected))
        }
      } +
        testM("executeBatchAction") {
          import ExecuteTestTable._
          checkM(Gen.listOfN(10)(gen)) { actual =>
            val truncateData = ZioCassandraSession.executeAction(truncate(batchTable))
            val toInsert: Action.Batch = {
              val inserts = actual.map(ExecuteTestTable.insert(batchTable))
              Action.Batch.unlogged(inserts.head, inserts.tail: _*)
            }
            val expected = ZioCassandraSession
              .select(selectAllIn(batchTable)(actual.map(_.id)))
              .runCollect

            for {
              _        <- truncateData
              _        <- ZioCassandraSession.executeBatchAction(toInsert)
              expected <- expected
            } yield assert(actual)(hasSameElements(expected))
          }
        }
    }

  // Used to provide a similar API as the `select` method
  private def selectPageStream[ScalaType](
    query: Query[ScalaType],
    executionAttributes: ExecutionAttributes
  ): ZStream[Has[ZioCassandraSession], Throwable, ScalaType] =
    ZStream
      .fromEffect(ZioCassandraSession.selectPage(input = query, page = None, config = executionAttributes))
      .flatMap {
        case (chunk, Some(page)) =>
          ZStream.fromChunk(chunk) ++
            ZStream.paginateChunkM(page)(nextPage =>
              ZioCassandraSession.selectPage(input = query, page = Some(nextPage), config = executionAttributes)
            )

        case (chunk, None) =>
          ZStream.fromChunk(chunk)
      }
}

final case class SystemLocalResponse(`system.now()`: UUID) {
  def time: Either[Throwable, Long] =
    Try(Uuids.unixTimestamp(`system.now()`)).toEither
}
object SystemLocalResponse {
  implicit val readerForSystemLocalResponse: Reader[SystemLocalResponse] =
    Reader.derive[SystemLocalResponse]
}

final case class PreparedStatementsResponse(
  prepared_id: ByteBuffer,
  logged_keyspace: Option[String],
  query_string: String
)
object PreparedStatementsResponse {
  implicit val readerForPreparedStatementsResponse: Reader[PreparedStatementsResponse] =
    Reader.derive[PreparedStatementsResponse]
}

final case class ExecuteTestTable(id: Int, info: String)
object ExecuteTestTable {
  implicit val readerForExecuteTestTable: Reader[ExecuteTestTable] = Reader.derive[ExecuteTestTable]
  implicit val writerForExecuteTestTable: Writer[ExecuteTestTable] = Writer.derive[ExecuteTestTable]

  val table      = "ziocassandrasessionspec_executeAction"
  val batchTable = "ziocassandrasessionspec_executeBatchAction"

  def truncate(tbl: String) = s"TRUNCATE $tbl"

  val gen: Gen[Random with Sized, ExecuteTestTable] = for {
    id   <- Gen.int(1, 1000)
    info <- Gen.alphaNumericStringBounded(10, 15)
  } yield ExecuteTestTable(id, info)

  def insert(table: String)(in: ExecuteTestTable): Action.Single =
    (cql"INSERT INTO ".appendString(table) ++ cql"(id, info) VALUES (${in.id}, ${in.info})").action

  def selectAllIn(table: String)(ids: List[Int]): Query[ExecuteTestTable] =
    (cql"SELECT id, info FROM ".appendString(table) ++ cql" WHERE id IN $ids")
      .query[ExecuteTestTable]
}

final case class SelectPageRow(id: Int, bucket: Int, info: String)
object SelectPageRow {
  implicit val readerForSelectPageRow: Reader[SelectPageRow] = Reader.derive[SelectPageRow]
  implicit val writerForSelectPageRow: Writer[SelectPageRow] = Writer.derive[SelectPageRow]

  val truncate = s"TRUNCATE ziocassandrasessionspec_selectPage"

  def insert(in: SelectPageRow): Action.Single =
    cql"INSERT INTO ziocassandrasessionspec_selectPage (id, bucket, info) VALUES (${in.id}, ${in.bucket}, ${in.info})".action

  def selectAll: Query[SelectPageRow] =
    cql"SELECT id, bucket, info FROM ziocassandrasessionspec_selectPage".query[SelectPageRow]

  val gen: Gen[Random with Sized, SelectPageRow] =
    for {
      id     <- Gen.int(1, 1000)
      bucket <- Gen.int(1, 50)
      info   <- Gen.alphaNumericStringBounded(10, 15)
    } yield SelectPageRow(id, bucket, info)
}