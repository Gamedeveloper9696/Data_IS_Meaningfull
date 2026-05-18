import  {test,expect} from '@playwright/test'
test('titletest', async({page}) => {
await page.goto("https://orangetesting.com/");
await page.locator("body > header > nav > a").getByLabel()

});