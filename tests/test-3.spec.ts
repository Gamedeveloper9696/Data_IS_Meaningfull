import { test, expect } from '@playwright/test';

test('test', async ({ page }) => {
  await page.goto('https://orangehrm.com/30-day-free-trial');
  await page.getByRole('textbox', { name: 'Full Name' }).click();
  await page.getByRole('textbox', { name: 'Full Name' }).fill('Poornima');
  await page.locator('#Form_getForm_action_submitForm').click();
  await page.getByRole('textbox', { name: 'Name for the Trial System' }).click();
  await page.getByRole('textbox', { name: 'Name for the Trial System' }).click();
});