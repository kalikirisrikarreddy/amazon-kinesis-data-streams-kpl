package amazon.kinesis;

public class Employee {
	private String id;
	private String name;
	private String designation;
	private String company;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDesignation() {
		return designation;
	}

	public void setDesignation(String designation) {
		this.designation = designation;
	}

	public String getCompany() {
		return company;
	}

	public void setCompany(String company) {
		this.company = company;
	}

	public Employee() {
		super();
	}

	public Employee(String id, String name, String designation, String company) {
		super();
		this.id = id;
		this.name = name;
		this.designation = designation;
		this.company = company;
	}

}
