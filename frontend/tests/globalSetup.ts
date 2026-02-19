import { execSync } from "child_process";
import path from "path";

async function globalSetup() {
  console.log("Global Setup: Running factory reset and infrastructure startup...");
  try {
    const scriptPath = path.resolve(__dirname, "../../scripts/setup-e2e.sh");
    execSync(scriptPath, { stdio: "inherit" });
    console.log("Global Setup: Complete.");
  } catch (error) {
    console.error("Global Setup: Failed.");
    throw error;
  }
}

export default globalSetup;
