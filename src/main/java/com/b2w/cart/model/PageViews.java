package com.b2w.cart.model;

import java.io.Serializable;

public class PageViews implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String timestamp;
	private String customer;
	private String page;
	private String product;
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public String getCustomer() {
		return customer;
	}
	public void setCustomer(String customer) {
		this.customer = customer;
	}
	public String getPage() {
		return page;
	}
	public void setPage(String page) {
		this.page = page;
	}
	public String getProduct() {
		return product;
	}
	public void setProduct(String product) {
		this.product = product;
	}
	@Override
	public String toString() {
		return "PageViews [timestamp=" + timestamp + ", customer=" + customer + ", page=" + page + ", product="
				+ product + "]";
	}
	
	
}
