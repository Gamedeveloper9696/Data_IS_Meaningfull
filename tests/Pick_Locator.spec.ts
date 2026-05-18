import { test, expect, chromium } from '@playwright/test';

test('test', async ({ page }) => {
     const browser= await chromium.launch({headless:false});
     const context1=await browser.newContext();
     const page1=await browser.newPage();
     const username='admin';
        const password='admin';
        const authheader='Basic '+ btoa(username+':'+password);
        page1.setExtraHTTPHeaders({Authorization: authheader});
       await page1.goto('https://the-internet.herokuapp.com/basic_auth');
     //the below linke user name admin and password admin is given in the url to provide basic authetication to enter url

     //since hardcded, setthttp header and pass the credentials in the header to avoid hardcoding in the url
    // await page1.goto('https://admin:admin@the-internet.herokuapp.com/basic_auth');
 //other way of doing it with function

 //username='admin';
   // password='admin';
  
 //page1.setExtraHTTPHeaders({Authorization: createauth(username,password)});
//await page1.goto('https://the-internet.herokuapp.com/basic_auth');
 //function createauth(username any,password any)
 //{
 //   return 'Basic '+ btoa(username+':'+password);
 //}
   

    
});