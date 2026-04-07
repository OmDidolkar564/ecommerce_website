import fs from 'fs';
import path from 'path';
import csv from 'csv-parser';
import axios from 'axios';
import { AppDataSource } from '../data-source';
import { Product } from '../entities/Product';
import { Type } from '../entities/Type';
import { Category } from '../entities/Category';
import { SubCategory } from '../entities/SubCategory';

// ---------- HELPERS ----------

function cleanPrice(priceStr: string): number {
  if (!priceStr) return 0;
  return parseFloat(priceStr.replace(/[^0-9.]/g, '')) || 0;
}

async function downloadImage(url: string, filepath: string) {
  try {
    const response = await axios({
      url,
      responseType: 'stream',
      timeout: 5000
    });

    return new Promise((resolve, reject) => {
      const writer = fs.createWriteStream(filepath);
      response.data.pipe(writer);
      writer.on('finish', resolve);
      writer.on('error', reject);
    });
  } catch {
    return false;
  }
}

// ---------- MAIN PROCESS ----------

async function processRow(row: any, countRef: { count: number }) {
  try {
    const name = row.name?.trim();
    const typeName = row.main_category?.trim();
    const subCategoryName = row.sub_category?.trim();

    if (!name || !typeName || !subCategoryName) return;

    // PRICE
    const price = cleanPrice(row.discount_price);
    const mrp = cleanPrice(row.actual_price);

    // RATINGS
    const rating = parseFloat(row.ratings) || 0;
    const ratingCount = parseInt(row["no of ratings"]) || 0;

    // CATEGORY FIX
    const categoryName = typeName + "_general";

    const typeRepo = AppDataSource.getRepository(Type);
    const categoryRepo = AppDataSource.getRepository(Category);
    const subRepo = AppDataSource.getRepository(SubCategory);
    const productRepo = AppDataSource.getRepository(Product);

    // ---------- TYPE ----------
    let type = await typeRepo.findOne({ where: { name: typeName } });
    if (!type) {
      type = await typeRepo.save({ name: typeName });
    }

    // ---------- CATEGORY ----------
    let category = await categoryRepo.findOne({
      where: { name: categoryName, type: { id: type.id } }
    });
    if (!category) {
      category = await categoryRepo.save({ name: categoryName, type });
    }

    // ---------- SUBCATEGORY ----------
    let subCategory = await subRepo.findOne({
      where: { name: subCategoryName, category: { id: category.id } }
    });
    if (!subCategory) {
      subCategory = await subRepo.save({
        name: subCategoryName,
        category
      });
    }

    // ---------- DUPLICATE CHECK ----------
    const existing = await productRepo.findOne({ where: { name } });
    if (existing) return;

    // ---------- IMAGE ----------
    let imagePath = '/images/default.jpg';
    const imageUrl = row.image;

    if (imageUrl) {
      const fileName = `product_${Date.now()}_${Math.floor(Math.random() * 1000)}.jpg`;
      const localPath = path.join('ProductImages', fileName);

      const success = await downloadImage(imageUrl, localPath);
      if (success !== false) {
        imagePath = `/images/${fileName}`;
      }
    }

    // ---------- INSERT PRODUCT ----------
    await productRepo.save({
      name,
      description: name,
      price,
      mrp,
      rating,
      ratingCount,
      image: imagePath,
      subCategory
    });

    countRef.count++;

    if (countRef.count % 50 === 0) {
      console.log(`Inserted ${countRef.count} products`);
    }

  } catch (err: any) {
    console.error("Row Error:", err.message);
  }
}

// ---------- RUN SCRIPT ----------

async function run() {
  await AppDataSource.initialize();

  const dataFolder = path.join(__dirname, '../data');
  const files = fs.readdirSync(dataFolder).filter(f => f.endsWith('.csv'));

  console.log(`Found ${files.length} CSV files`);

  const countRef = { count: 0 };

  for (const file of files) {
    const filePath = path.join(dataFolder, file);
    console.log(`\nProcessing file: ${file}`);

    const rows: any[] = [];

    await new Promise<void>((resolve, reject) => {
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => rows.push(row))
        .on('end', async () => {
          for (const row of rows) {
            await processRow(row, countRef);
          }
          resolve();
        })
        .on('error', reject);
    });
  }

  console.log(`\n✅ Import completed. Total products: ${countRef.count}`);
}

run();