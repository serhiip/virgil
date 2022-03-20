package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.RelationSpec_Person._
import io.kaizensolutions.virgil.cql.CqlInterpolatedStringOpsForString
import io.kaizensolutions.virgil.dsl._
import zio.Has
import zio.random.Random
import zio.test.TestAspect.{samples, sequential}
import zio.test._

object RelationSpec {
  def relationSpec: ZSpec[TestConfig with Random with Has[CQLExecutor], Any] =
    suite("Relational Operators Specification") {
      testM("isNull") {
        checkM(gen) { person =>
          val update =
            UpdateBuilder(table)
              .set(Name := person.name)
              .set(Age := person.age)
              .where(Id === person.id)
              .ifCondition(Name.isNull)
              .build
              .execute
              .runDrain

          val find = RelationSpec_Person.find(person.id).execute.runHead.some

          truncate.execute.runDrain *>
            update *>
            find.map(actual => assertTrue(actual == person))
        }
      } + testM("isNotNull") {
        checkM(RelationSpec_Person.gen) { person =>
          val insert  = RelationSpec_Person.insert(person).execute.runDrain
          val newName = person.name + " " + person.name
          val update =
            UpdateBuilder(RelationSpec_Person.table)
              .set(Name := newName)
              .where(Id === person.id)
              .ifCondition(Name.isNotNull)
              .build
              .execute
              .runDrain

          val find = RelationSpec_Person.find(person.id).execute.runHead.some

          truncate.execute.runDrain *>
            insert *>
            update *>
            find.map(actual => assertTrue(actual == person.copy(name = newName)))
        }
      }
    } @@ sequential @@ samples(4)
}

final case class RelationSpec_Person(
  id: Int,
  name: String,
  age: Int
)
object RelationSpec_Person {
  val Id   = "id"
  val Name = "name"
  val Age  = "age"

  val table: String = "relationspec_person"

  val truncate: CQL[MutationResult] = s"TRUNCATE TABLE $table".asCql.mutation

  def insert(in: RelationSpec_Person): CQL[MutationResult] =
    InsertBuilder(table)
      .value(Id, in.id)
      .value(Name, in.name)
      .value(Age, in.age)
      .build

  def find(id: Int): CQL[RelationSpec_Person] =
    SelectBuilder
      .from(table)
      .columns(Id, Name, Age)
      .where(Id === id)
      .build[RelationSpec_Person]

  def gen: Gen[Random, RelationSpec_Person] =
    for {
      id   <- Gen.int(1, 1000)
      name <- Gen.stringBounded(2, 4)(Gen.alphaChar)
      age  <- Gen.int(1, 100)
    } yield RelationSpec_Person(id, name, age)
}
