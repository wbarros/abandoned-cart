package com.b2w.cart;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public class AbandonedCart {
	
	public static void main(String[] args) {
	
		 PipelineOptions options = PipelineOptionsFactory.create();
		 Pipeline p = Pipeline.create(options);
		 PCollection<String> input = p.apply(TextIO.read().from("C:\\input\\page-views.json"));
		 
		 PCollection<String> sysout = input.apply("teste", MapElements.via(
				 new SimpleFunction<String, String>() {
					 @Override
					 public String apply(String input) {
						 System.out.println(input);
						 return "OK";
					 }
				 }));
		 
		 PCollection<String> resultado = sysout.apply("Resultado", ParDo.of(new DoFn<String, String>() {
			 
			 @ProcessElement
			 public void processElement(ProcessContext context) {
				 context.output(context.element());
			 }
		 }));
		 
		 resultado.apply(TextIO.write().to("/tmp/beam/cars_sales_report").withoutSharding());
		 
		 p.run().waitUntilFinish();
	}
}
