package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.CollectionsSpecDatatypes.*

trait TupleCollectionRowInstances:
  given cqlRowDecoderForOptionCollectionRow: CqlRowDecoder.Object[TupleCollectionRow] =
    CqlRowDecoder.derive[TupleCollectionRow]
