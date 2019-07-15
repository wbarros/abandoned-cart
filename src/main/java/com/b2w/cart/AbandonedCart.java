package com.b2w.cart;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import com.b2w.cart.model.PageViews;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class AbandonedCart {
	
	public static void main(String[] args) {
	
		 PipelineOptions options = PipelineOptionsFactory.create();
		 Pipeline p = Pipeline.create(options);
		 PCollection<String> input = p.apply(TextIO.read().from("input/page-views.json"));
		 
		 PCollection<PageViews> sysout = input.apply("teste", MapElements.via(
				 new SimpleFunction<String, PageViews>() {
					 @Override
					 public PageViews apply(String input) {
						 System.out.println(input);
						 GsonBuilder builder = new GsonBuilder();
						 Gson gson = builder.create();
						 PageViews pageViews = gson.fromJson(input, PageViews.class);
						 return pageViews;
					 }
				 }));
		 
		 PCollection<String> resultado = sysout.apply("Resultado", ParDo.of(new DoFn<PageViews, String>() {
			 List<PageViews> listaPageViews = new ArrayList<>();
			 @ProcessElement
			 public void processElement(ProcessContext context) {
				 
				 listaPageViews.add(context.element());
				 
				 //System.out.println(listaPageViews.toString());
				 
				 context.output(context.element().toString());
			 }
			 
		 }));
		 
		 resultado.apply(TextIO.write().to("output/abandoned-carts.json").withoutSharding());
		 
		 p.run().waitUntilFinish();
	}
}
