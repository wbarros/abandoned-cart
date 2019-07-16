package com.b2w.cart;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import javax.xml.crypto.dsig.keyinfo.PGPData;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.b2w.cart.model.PageViews;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class AbandonedCart {
	
	public static void main(String[] args) {
	
		 PipelineOptions options = PipelineOptionsFactory.create();
		 Pipeline p = Pipeline.create(options);
		 PCollection<String> input = p.apply(TextIO.read().from("input/page-views.json"));
		 
		 PCollection<KV<String, PageViews>> jsonParseToPageViews = input.apply("teste", MapElements.via(
				 new SimpleFunction<String, KV<String, PageViews>>() {
					 @Override
					 public KV<String, PageViews> apply(String input) {
						 //System.out.println(input);
						 GsonBuilder builder = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss");
						 Gson gson = builder.create();
						 PageViews pageViews = gson.fromJson(input, PageViews.class);
						 String key = pageViews.getCustomer();
						 return KV.of(key, pageViews);
					 }
				 }));
		 
		 PCollection<KV<String, Iterable<PageViews>>> kvpCollection =
				 jsonParseToPageViews.apply(GroupByKey.<String, PageViews>create());
		 
		 PCollection<String> resultado = kvpCollection.apply("Resultado", ParDo.of(new DoFn<KV<String, Iterable<PageViews>>, String>() {
			 @ProcessElement
			 public void processElement(ProcessContext context) {
				 Boolean isAbandoned = false;
				 String key = context.element().getKey();
				 Iterable<PageViews> pageViews = context.element().getValue();
				 List<PageViews> listCustomerN = new ArrayList<>();
				 for(PageViews page : pageViews) {
					 System.out.println(page.toString());
					 if(key.equals(page.getCustomer()))
						 listCustomerN.add(page);
				 }
				 List<PageViews> sortedList = listCustomerN.stream()
							.sorted(Comparator.comparing(PageViews::getTimestamp))
							.collect(Collectors.toList());
				 
				 if(sortedList.size() > 1) {
					 
					 for(int i = 1; i < sortedList.size(); i++) {
					 	if(sortedList.get(i).getPage().equals("basket")) {
					 		if( i >= 1 && (((sortedList.get(i).getTimestamp().getTime() - sortedList.get(i-1).getTimestamp().getTime())/1000)/60) >= 10 ) {
					 			context.output(sortedList.get(i).toString());
					 		} 
					 		else if(i < sortedList.size() -1 && (((sortedList.get(i+1).getTimestamp().getTime() - sortedList.get(i).getTimestamp().getTime())/1000)/60) >= 10 ) {
					 			context.output(sortedList.get(i+1).toString());
					 		} else if (i+1 == sortedList.size()) {
					 			context.output(sortedList.get(i).toString());
					 		}
					 		
					 	}
					 }
				 }
				 
//				 if(isAbandoned)
//				 	context.output("");
					 
				 
			 }
			 
		 }));
		 
		 resultado.apply(TextIO.write().to("output/abandoned-carts.json").withoutSharding());
		 
		 p.run().waitUntilFinish();
	}
}
