import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';
const proxy={'/api':{target:'http://backend:8080',changeOrigin:true}};
export default defineConfig({plugins:[vue()],server:{proxy},preview:{proxy,allowedHosts:['localhost','127.0.0.1']}});
