import { test, expect, chromium, Browser, Page, BrowserContext } from '@playwright/test';

test('has title', async ({  }) => {

  const browser:Browser = await chromium.launch();
  const browsercontext_1=await browser.newContext();
  const page=await browsercontext_1.newPage();
  page.waitForTimeout(5000);
  await page.goto('https://naveenautomationlabs.com/opencart/index.php?route=account/login');
  const username1=await page.locator('#input-email');
  const password1=await page.locator('#input-password')
  const loginbutton1=await page.locator("[value='Login']");
  await username1.fill('poornisubramani@gmail.com');
  await password1.fill('Srisairam3*');
  await loginbutton1.click();
  console.log(await page.title());
  expect(await page.title()).toEqual('Account Login');
  // no explicit browser variable in Playwright test fixtures; page will be closed by the test runner
  await browsercontext_1.close();
  await browser.close();
});
