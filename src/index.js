import {loadAndCombineCsvAndJson} from "./service/status-csv-entries-read.js";


const fileName = process.env.USE_FILE ?? '';

(async () => {
  console.log('Running CSV App');

  try {
    await loadAndCombineCsvAndJson(fileName);

    console.log('All done, have a nice day');
  } catch (error) {
    console.log(`A fatal error occurred: ${error}`);
  }

})();
