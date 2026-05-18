import {test,expect,} from '@playwright/test';
test.describe('my first  test suite',()=>{
  test('test1', async ({page})=>{
   const url=await page.goto('https://naveenautomationlabs.com/opencart/index.php?route=account/login');
   const username=await page.locator('#input-email');
   const password=await page.locator('#input-password')
   const loginbutton=page.locator("[value='Login']");
   await username.fill('poornisubramani@gmail.com');
   await password.fill('Srisairam3*');
   await loginbutton.click();
  
   console.log(await page.title());
   //await expect(page).toHaveTitle('Account Login');
   await page.screenshot({path:'screenshot.png', fullPage:true});

  });
});
