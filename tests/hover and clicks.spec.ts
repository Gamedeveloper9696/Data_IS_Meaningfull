import {test,expect,Browser,Page, chromium} from '@playwright/test';


  // test('hover', async ({  }) => {
   
  //   // ensure the element is visible before hovering
  //   const browser:Browser= await chromium.launch({ headless: false, channel: 'chrome' });
  //   const browsercontext=await browser.newContext({permissions:['notifications']});
  //   const page=await browsercontext.newPage();
  //   await page.goto('https://www.spicejet.com/');
  //    await page.getByText('Add-ons').first().hover();
  //   //await spiceClub.waitFor({ state: 'visible' });
    
  //   const ourProgram = page.getByText('Visa ').first();
  //   //await ourProgram.waitFor({ state: 'visible' });
  //   await ourProgram.click();
  
    
  //  });

  //  test('click', async ({  }) => {
   
  //   // ensure the element is visible before hovering
  //   const browser= await chromium.launch({ headless: false, channel: 'chrome' });
  //   const browsercontext=await browser.newContext({permissions:['notifications']});
  //   const page=await browsercontext.newPage();
  //   await page.goto('https://demo.guru99.com/test/simple_context_menu.html');
  //    await page.getByText('Double-Click Me To See Alert').dblclick();
  //   //await spiceClub.waitFor({ state: 'visible' });
  //   await page.getByText('right click me').click({button:'right'});
  //   await page.goto('https://the-internet.herokuapp.com/shifting_content');
  //    page.getByText('Example 1: Menu Element ').click({modifiers:["Shift"]});
  //   //await ourProgram.waitFor({ state: 'visible' });
    
  //   await page.waitForTimeout(5000);
    
  //  });