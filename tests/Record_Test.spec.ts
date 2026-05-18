import { test, expect } from '@playwright/test';

test('test', async ({ page }) => {

  await test.step('Navigate to URL', async()=> {
      await page.goto('https://github.com/');
       await page.getByRole('link', { name: 'Sign in' }).click();
  });
 
  await  test.step('Enter user name and password', async()=> {
        await page.getByRole('textbox', { name: 'Username or email address' }).click();
       await page.getByRole('textbox', { name: 'Username or email address' }).fill('TEST');
      await page.getByRole('textbox', { name: 'Password' }).click();
      await page.getByRole('textbox', { name: 'Password' }).fill('tEST123');
           
  }); 
 
});