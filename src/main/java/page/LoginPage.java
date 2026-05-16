package page;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;

public class LoginPage {

	private WebDriver driver;
	
	private By usernameText=By.id("Email");
	private By password=By.cssSelector("#Password");
    private By loginButton=By.xpath("//*[@id=\"main\"]/div/section/div/div[2]/div[1]/div/form/div[3]/button");
	
    public LoginPage(WebDriver driver) {
    	this.driver=driver;
    }
    public void enterusername(String username) {
    	driver.findElement(usernameText).clear();
    	driver.findElement(usernameText).sendKeys(username);
    	    	
    }
    
    public void enterpassword(String pass) {
    	driver.findElement(password).sendKeys(pass);
    }
    
    public void clicklogin() {
    	driver.findElement(loginButton).click();
    	
    }
	
}
