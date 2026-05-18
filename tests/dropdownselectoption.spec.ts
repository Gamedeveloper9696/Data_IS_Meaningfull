import { test, expect, chromium, Browser, Page, BrowserContext } from '@playwright/test';

test('has title', async ({ page }) => {

await page.goto("https://magupdate.co.uk/magazine-subscription/phrr");
await page.locator('#Contact_CountryCode').selectOption('India');
const valuesdropdown= await page.$$('#Contact_CountryCode' + '> option')
  console.log(valuesdropdown.length);
for (const e of valuesdropdown ){
    const text=await e.textContent();
    console.log( text);
    if (await text == 'Argentina'){
        console.log('Argentina is present in the dropdown');
        await page.locator('#Contact_CountryCode').selectOption({label: 'Argentina'});
        break;
     }
    }
     
});

