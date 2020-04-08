package net.codingblocks

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptors

fun main(args: Array<String>) {
    val p = Pipeline.create()
    p
            .apply<PCollection<String>>(TextIO.read().from("pom.xml"))
            .apply(
                    FlatMapElements.into(TypeDescriptors.strings())
                            .via(ProcessFunction<String, List<String>> { input -> input.toList().map { it.toString() } })
            )
            .apply(Count.perElement<String>())
            .apply(
                    MapElements.into(TypeDescriptors.strings())
                            .via(ProcessFunction { input -> "${input.key} : ${input.value}" })
            )
            .apply(TextIO.write().to("counts"))

    p.run()
}
