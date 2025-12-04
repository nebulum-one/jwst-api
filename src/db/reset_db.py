from src.db.database import Base, engine

def reset_db():
    print("Dropping all tables...")
    Base.metadata.drop_all(bind=engine)

    print("Creating all tables...")
    Base.metadata.create_all(bind=engine)

    print("Database reset complete.")

if __name__ == "__main__":
    reset_db()
    