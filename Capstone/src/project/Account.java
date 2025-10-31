package project;

public class Account {
	private long accountNumber;
	private long customerId;
	private String accountType;
	private String branch;

	public Account() {}
	public Account(long accountNumber, int customerId, String accountType, String branch) {
		super();
		this.accountNumber = accountNumber;
		this.customerId = customerId;
		this.accountType = accountType;
		this.branch = branch;
	}
	public long getAccountNumber() {
		return accountNumber;
	}
	public void setAccountNumber(long accountNumber) {
		this.accountNumber = accountNumber;
	}
	public long getCustomerId() {
		return customerId;
	}
	public void setCustomerId(int customerId) {
		this.customerId = customerId;
	}
	public String getAccountType() {
		return accountType;
	}
	public void setAccountType(String accountType) {
		this.accountType = accountType;
	}
	public String getBranch() {
		return branch;
	}
	public void setBranch(String branch) {
		this.branch = branch;
	}



}
