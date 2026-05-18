import {test,expect,Browser,Page} from '@playwright/test';

test.describe('my first test suite', () => {
  test('test1', async ({ page }) => {
    page.setDefaultTimeout(30000);
    await page.goto('https://naveenautomationlabs.com/opencart/index.php?route=account/register',{waitUntil: 'networkidle'});
    expect(await page.getByRole('heading', {name:'Register Account'})). toBeVisible();
//getByRole('checkbox')->without the name we can use.
//similiar to button and links
//open browser in non incognito mode and check the role of the element in the dev tools and use that role in the code to locate the element
//const browser=await page.launchpersistentcontext('', {headless:false},channel='chrome');
  
//const pages=browser.pages();
//const page= pages[0]
})
});