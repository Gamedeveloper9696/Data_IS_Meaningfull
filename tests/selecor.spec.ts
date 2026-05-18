import { test, expect } from '@playwright/test';

test.describe('my first test suite', () => {
  test('test1', async ({ page }) => {
    page.setDefaultTimeout(30000);
    await page.goto('https://naveenautomationlabs.com/opencart/index.php?route=account/register',{waitUntil: 'networkidle'});
    const firstname = await page.locator('#input-firstname');
    await firstname.fill('poorni', {timeout:5000});

    const lastname = await page.locator('//input[@type="text"]').nth(1);
    await lastname.fill('subramani');

    const email = await page.locator('input[type="email"]');
    await email.fill('poornisubramani@gmail.com', {timeout:5000});

    //getbyTestid if the webelement name is data test id
    //await page.getByTestId('username).fill('tes12');
  });
});