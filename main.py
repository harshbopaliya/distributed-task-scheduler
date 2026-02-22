import uvicorn
import config


def main():
    print(f"Starting server on {config.API_HOST}:{config.API_PORT}")
    print(f"Docs: http://localhost:{config.API_PORT}/docs")

    uvicorn.run(
        "api:app",
        host=config.API_HOST,
        port=config.API_PORT,
        log_level="info"
    )


if __name__ == "__main__":
    main()
