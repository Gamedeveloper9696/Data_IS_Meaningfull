package test;

import org.testng.Assert;
import org.testng.annotations.Test;

import Base.BaseTest;
import page.LoginPage;

public class LoginTest extends BaseTest{
	
	@Test
	public void testValidlogin() {
	

	LoginPage LoginPage = new LoginPage(driver);
	
	LoginPage.enterusername("admin@yourstore.com");
	LoginPage.enterpassword("admin");
	LoginPage.clicklogin();
	
	System.out.println("my page title is: " +driver.getTitle());
	Assert.assertEquals(driver.getTitle(), "Just a moment...");
	
		
	
	
	}

}
