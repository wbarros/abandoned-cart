package com.b2w.cart.model;

import java.io.Serializable;
import java.util.Date;

import com.google.gson.annotations.Expose;

public class PageViews implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	@Expose
	private Date timestamp;
	@Expose
	private String customer;
	@Expose(serialize = false)
	private String page;
	@Expose
	private String product;
	
	public Date getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Date timestamp) {
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
