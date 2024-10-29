declare global {
    namespace NodeJS {
        interface ProcessEnv {
            BOX_DEVELOPER_TOKEN: string;
        }
    }
}

export {};
