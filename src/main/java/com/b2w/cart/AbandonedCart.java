package com.b2w.cart;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

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
	
	private final static String DATA_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private final static String PAGE_BASKET = "basket";
	private final static String INPUT_PATH = "input/page-views.json";
	private final static String OUTPUT_PATH = "output/abandoned-carts.json";
	
	public static void main(String[] args) {
	
		 PipelineOptions options = PipelineOptionsFactory.create();
		 Pipeline p = Pipeline.create(options);
		 PCollection<String> input = p.apply(TextIO.read().from(INPUT_PATH));
		 
		 PCollection<KV<String, PageViews>> jsonParseToPageViewsKV = input.apply("jsonParseToPageViewsKV", MapElements.via(
				 new SimpleFunction<String, KV<String, PageViews>>() {
					 @Override
					 public KV<String, PageViews> apply(String input) {
						 //System.out.println(input);
						 GsonBuilder builder = new GsonBuilder().setDateFormat(DATA_FORMAT);
						 Gson gson = builder.create();
						 PageViews pageViews = gson.fromJson(input, PageViews.class);
						 String key = pageViews.getCustomer();
						 return KV.of(key, pageViews);
					 }
				 }));
		 
		 PCollection<KV<String, Iterable<PageViews>>> kvpCollection =
				 jsonParseToPageViewsKV.apply(GroupByKey.<String, PageViews>create());
		 
		 PCollection<String> resultado = kvpCollection.apply("Resultado", ParDo.of(new DoFn<KV<String, Iterable<PageViews>>, String>() {

			 private Gson gson;
			 
			 @ProcessElement
			 public void processElement(ProcessContext context) {
				 String key = context.element().getKey();
				 Iterable<PageViews> pageViews = context.element().getValue();
				 List<PageViews> listCustomerN = new ArrayList<>();
				 for(PageViews page : pageViews) {
//					 System.out.println(page.toString());
					 if(key.equals(page.getCustomer()))
						 listCustomerN.add(page);
				 }
				 List<PageViews> sortedList = listCustomerN.stream()
							.sorted(Comparator.comparing(PageViews::getTimestamp))
							.collect(Collectors.toList());
				 
				 if(sortedList.size() > 1) {
					 gson = new GsonBuilder().setDateFormat(DATA_FORMAT).excludeFieldsWithoutExposeAnnotation().create();
					 for(int i = 1; i < sortedList.size(); i++) {
					 	if(sortedList.get(i).getPage().equals(PAGE_BASKET)) {
					 		if( i >= 1 ) { 
					 			Long tempoVisualizacaoAtual = sortedList.get(i).getTimestamp().getTime();
					 			Long tempoVisualizacaoAnterior = sortedList.get(i-1).getTimestamp().getTime();
					 			Long diferencaTempo = miliSegundosParaMinutos(tempoVisualizacaoAtual, tempoVisualizacaoAnterior);
					 			if( diferencaTempo >= 10 ) {
					 				context.output(gson.toJson(sortedList.get(i)));
		 						}
					 		} 
					 		if(i < sortedList.size() -1) {
					 			Long tempoVisualizacaoProximo = sortedList.get(i+1).getTimestamp().getTime();
					 			Long tempoVisualizacaoAtual = sortedList.get(i).getTimestamp().getTime();
					 			Long diferencaTempo = miliSegundosParaMinutos(tempoVisualizacaoProximo, tempoVisualizacaoAtual);
					 			if( diferencaTempo >= 10 ) {
					 				context.output(gson.toJson(sortedList.get(i)));
	 							}	
					 		} if (i+1 == sortedList.size()) {
					 			context.output(gson.toJson(sortedList.get(i)));
					 		}
					 		
					 	}
					 }
				 }
				 
			 }
			 
		 }));
		 
		 resultado.apply(TextIO.write().to(OUTPUT_PATH).withoutSharding());
		 
		 p.run().waitUntilFinish();
	}
	
	public static Long miliSegundosParaMinutos(Long tempo, Long tempoAnterior) {
		return ((tempo - tempoAnterior)/1000)/60;
	}
}
