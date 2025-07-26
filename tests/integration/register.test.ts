import axios from "axios";

import { getApiUrl } from "./test-utils";

describe("POST /agents (integration)", () => {
  let apiUrl: string;

  beforeEach(async () => {
    apiUrl = await getApiUrl();
  });

  it("returns 400 if missing resourceArn", async () => {
    try {
      await axios.post(apiUrl + "agents", {});
    } catch (error: any) {
      expect(error.response.status).toBe(400);
      expect(error.response.data).toEqual({ error: "resourceArn is required" });
    }
  });

  it("registers a new agent", async () => {
    const arn = "arn:aws:test:my-resource";
    const response = await axios.post(apiUrl + "agents", { resourceArn: arn });

    expect(response.status).toBe(200);
    expect(response.data).toHaveProperty("agentId");
    expect(response.data).toHaveProperty("queueUrl");
    expect(response.data.resourceArn).toBe(arn);
  });

  it("returns same agent on duplicate registration", async () => {
    const arn = "arn:aws:test:my-resource";
    const first_response = await axios.post(apiUrl + "agents", {
      resourceArn: arn,
    });
    const second_response = await axios.post(apiUrl + "agents", {
      resourceArn: arn,
    });

    expect(second_response.status).toBe(200);
    expect(second_response.data.agentId).toBe(first_response.data.agentId);
    expect(second_response.data.queueUrl).toBe(first_response.data.queueUrl);
  });
});
